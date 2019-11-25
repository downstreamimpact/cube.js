const R = require('ramda');
const chrono = require('chrono-node');
const moment = require('moment-timezone');
const UserError = require('../compiler/UserError');


class PreAggregations {
  constructor(query, historyQueries, cubeLatticeCache) {
    this.query = query;
    this.historyQueries = historyQueries;
    this.cubeLatticeCache = cubeLatticeCache;
    this.cubeLattices = {};
  }

  preAggregationsDescription() {
    return R.pipe(R.unnest, R.uniqBy(desc => desc.tableName))(
      [this.preAggregationsDescriptionLocal()].concat(
        this.query.subQueryDimensions.map(d => this.query.subQueryDescription(d).subQuery)
          .map(q => q.preAggregations.preAggregationsDescription())
      )
    );
  }

  preAggregationsDescriptionLocal() {
    const preAggregationForQuery = this.findPreAggregationForQuery();
    if (preAggregationForQuery) {
      if (preAggregationForQuery.preAggregation.useOriginalSqlPreAggregations) {
        const { preAggregations, result } =
          this.collectOriginalSqlPreAggregations(() =>
            this.preAggregationDescriptionsFor(preAggregationForQuery.cube, preAggregationForQuery)
          );
        return R.unnest(preAggregations.map(p => this.preAggregationDescriptionsFor(p.cube, p))).concat(result);
      }
      return this.preAggregationDescriptionsFor(preAggregationForQuery.cube, preAggregationForQuery);
    }
    return R.pipe(
      R.map(cube => {
        const foundPreAggregation = this.findPreAggregationToUseForCube(cube);
        if (foundPreAggregation) {
          return this.preAggregationDescriptionsFor(cube, foundPreAggregation);
        }
        return null;
      }),
      R.filter(R.identity),
      R.unnest
    )(this.preAggregationCubes());
  }

  preAggregationCubes() {
    const join = this.query.join;
    return join.joins.map(j => j.originalTo).concat([join.root]);
  }

  preAggregationDescriptionsFor(cube, foundPreAggregation) {
    if (foundPreAggregation.preAggregation.partitionGranularity && this.query.timeDimensions.length) {
      const { dimension, partitionDimension } = this.partitionDimension(foundPreAggregation);
      return partitionDimension.timeSeries().map(range =>
        this.preAggregationDescriptionFor(cube, this.addPartitionRangeTo(foundPreAggregation, dimension, range))
      );
    }
    if (foundPreAggregation.preAggregation.dateRange && this.query.timeDimensions.length){
      foundPreAggregation.preAggregation.dateRange = PreAggregations.parseDateRangeStringOrList(
          foundPreAggregation.preAggregation.dateRange,
          foundPreAggregation.preAggregation.timezone
      )
    }
    return [this.preAggregationDescriptionFor(cube, foundPreAggregation)];
  }

  addPartitionRangeTo(foundPreAggregation, dimension, range) {
    return Object.assign({}, foundPreAggregation, {
      preAggregation: Object.assign({}, foundPreAggregation.preAggregation, {
        partitionTimeDimensions: [{
          dimension,
          dateRange: range
        }],
      })
    });
  }

  partitionDimension(foundPreAggregation) {
    const dimension = this.query.timeDimensions[0].dimension;
    const partitionDimension = this.query.newTimeDimension({
      dimension,
      granularity: this.castGranularity(foundPreAggregation.preAggregation.partitionGranularity),
      dateRange: this.query.timeDimensions[0].dateRange
    });
    return { dimension, partitionDimension };
  }

  preAggregationDescriptionFor(cube, foundPreAggregation) {
    const { preAggregationName, preAggregation } = foundPreAggregation;
    const tableName = this.preAggregationTableName(cube, preAggregationName, preAggregation);
    return {
      preAggregationsSchema: this.query.preAggregationSchema(),
      tableName,
      loadSql: this.query.preAggregationLoadSql(cube, preAggregation, tableName),
      invalidateKeyQueries: this.query.preAggregationInvalidateKeyQueries(cube, preAggregation),
      external: preAggregation.external
    };
  }

  preAggregationTableName(cube, preAggregationName, preAggregation) {
    return this.query.preAggregationTableName(
      cube, preAggregationName + (
      preAggregation.partitionTimeDimensions ?
        preAggregation.partitionTimeDimensions[0].dateRange[0].replace('T00:00:00.000', '').replace(/-/g, '') :
        ''
    ));
  }

  findPreAggregationToUseForCube(cube) {
    const preAggregates = this.query.cubeEvaluator.preAggregationsForCube(cube);
    const originalSqlPreAggregations = R.pipe(
      R.toPairs,
      R.filter(([k, a]) => a.type === 'originalSql')
    )(preAggregates);
    if (originalSqlPreAggregations.length) {
      const [preAggregationName, preAggregation] = originalSqlPreAggregations[0];
      return {
        preAggregationName,
        preAggregation,
        cube
      };
    }
    return null;
  }

  static sortGranularTimeDimensions(timeDimensions) {
    return timeDimensions && R.pipe(
        R.filter(d => !!d.granularity),
        R.map(d => [d.dimension, d.granularity]),
        R.sortBy(R.prop(0))
    )(timeDimensions) || [];
  }

  static sortRangeTimeDimensions(timeDimensions, timezone) {
    return timeDimensions && R.pipe(
        R.filter(d => !!d.dateRange),
        R.map(d => {
          const dateRange = d.dateRange;
          const parsedRange = PreAggregations.parseDateRangeStringOrList(dateRange, timezone);
          console.log("sortRangeTimeDimensions", {d, dateRange, timezone})
          console.log("parsedRange", {parsedRange})
          return [d.dimension, parsedRange, timezone]
        }),
        R.sortBy(R.prop(0))
    )(timeDimensions) || [];
  }

  static transformQueryToCanUseForm(query) {
    const sortedDimensions = this.squashDimensions(query);
    const measures = (query.measures.concat(query.measureFilters));
    const measurePaths = R.uniq(measures.map(m => m.measure));
    const leafMeasurePaths =
      R.pipe(
        R.map(m => query.collectLeafMeasures(() => query.traverseSymbol(m))),
        R.unnest,
        R.uniq
      )(measures);

    const sortedGranularTimeDimensions = PreAggregations.sortGranularTimeDimensions(query.timeDimensions);
    const sortedRangeTimeDimensions = PreAggregations.sortRangeTimeDimensions(query.timeDimensions, query.timezone);
    const hasNoTimeDimensionsWithoutGranularity = !query.timeDimensions.filter(d => !d.granularity).length;

    const allFiltersWithinSelectedDimensions =
      R.all(d => query.dimensions.map(dim => dim.dimension).indexOf(d) !== -1)(
        query.filters.map(f => f.dimension)
      );

    const isAdditive = R.all(m => m.isAdditive(), query.measures);
    const leafMeasureAdditive = R.all(path => query.newMeasure(path).isAdditive(), leafMeasurePaths);

    console.log("transformQueryToCanUseForm end", {sortedDimensions, sortedGranularTimeDimensions, sortedRangeTimeDimensions})

    return {
      sortedDimensions,
      sortedGranularTimeDimensions,
      sortedRangeTimeDimensions,
      measures: measurePaths,
      leafMeasureAdditive,
      leafMeasures: leafMeasurePaths,
      hasNoTimeDimensionsWithoutGranularity,
      allFiltersWithinSelectedDimensions,
      isAdditive
    };
  }

  static transformedQueryToReferences(query) {
    return {
      measures: query.measures,
      dimensions: query.sortedDimensions,
      timeDimensions: query.sortedGranularTimeDimensions.map(([dimension, granularity]) => ({ dimension, granularity }))
    };
  }

  canUsePreAggregationFn(query, refs) {
    return PreAggregations.canUsePreAggregationForTransformedQueryFn(
      PreAggregations.transformQueryToCanUseForm(query), refs
    );
  }

  canUsePreAggregationAndCheckIfRefValid(query) {
    const transformedQuery = PreAggregations.transformQueryToCanUseForm(query);
    return (refs) => {
      return PreAggregations.canUsePreAggregationForTransformedQueryFn(
        transformedQuery, refs
      );
    };
  }

  checkAutoRollupPreAggregationValid(refs) {
    try {
      this.autoRollupPreAggregationQuery(null, refs); // TODO null
      return true;
    } catch (e) {
      if (e instanceof UserError || e.toString().indexOf('ReferenceError') !== -1) {
        return false;
      } else {
        throw e;
      }
    }
  }

  static parseDate(ds, timezone) {
    // From cubejs-api-gateway dateParser
    let momentRange;
    const dateString = ds.toLowerCase();
    if (dateString.match(/(this|last)\s+(day|week|month|year|quarter|hour|minute|second)/)) {
      const match = dateString.match(/(this|last)\s+(day|week|month|year|quarter|hour|minute|second)/);
      let start = moment.tz(timezone);
      let end = moment.tz(timezone);
      if (match[1] === 'last') {
        start = start.add(-1, match[2]);
        end = end.add(-1, match[2]);
      }
      const span = match[2] === 'week' ? 'isoWeek' : match[2];
      momentRange = [start.startOf(span), end.endOf(span)];
    } else if (dateString.match(/last\s+(\d+)\s+(day|week|month|year|quarter|hour|minute|second)/)) {
      const match = dateString.match(/last\s+(\d+)\s+(day|week|month|year|quarter|hour|minute|second)/);
      const span = match[2] === 'week' ? 'isoWeek' : match[2];
      momentRange = [
        moment.tz(timezone).add(-parseInt(match[1], 10) - 1, match[2]).startOf(span),
        moment.tz(timezone).add(-1, match[2]).endOf(span)
      ];
    } else if (dateString.match(/today/)) {
      momentRange = [moment.tz(timezone).startOf('day'), moment.tz(timezone).endOf('day')];
    } else if (dateString.match(/yesterday/)) {
      const yesterday = moment.tz(timezone).add(-1, 'day');
      momentRange = [moment(yesterday).startOf('day'), moment(yesterday).endOf('day')];
    } else {
      const results = chrono.parse(dateString);
      if (!results) {
        throw new UserError(`Can't parse date: '${dateString}'`);
      }
      momentRange = results[0].end ? [
        moment(results[0].start.moment()).tz(timezone).startOf('day'),
        moment(results[0].end.moment()).tz(timezone).endOf('day')
      ] : [
        moment(results[0].start.moment()).tz(timezone).startOf('day'),
        moment(results[0].start.moment()).tz(timezone).endOf('day')
      ];
    }
    return momentRange.map(d => d.format(moment.HTML5_FMT.DATETIME_LOCAL_MS));
  }

  static parseDateRangeStringOrList(dateRange, timezone) {
    let parsedRange;
    if (typeof dateRange === 'string') {
        parsedRange = PreAggregations.parseDate(dateRange, timezone);
    } else {
        parsedRange = dateRange && dateRange.length === 1 ? [dateRange[0], dateRange[0]] : dateRange;
        // if (parsedRange.length === 2){
        //   parsedRange = [moment(parsedRange[0]).format(moment.HTML5_FMT.DATETIME_LOCAL_MS), moment(parsedRange[0]).format('YYYY-MM-DD 23:59:59')]
        // } else {
          parsedRange = parsedRange.map(d => moment(d).format(moment.HTML5_FMT.DATETIME_LOCAL_MS))
        // }
    }
    // TODO - change second one to formatted with `.format('YYYY-MM-DD 23:59:59')` for EOD
    return parsedRange
  }

  static canUsePreAggregationForTransformedQueryFn(transformedQuery, refs) {
    console.log("canUsePreAggregationForTransformedQueryFn", {transformedQuery, refs})
    // TimeDimension :: [Dimension, Granularity]
    // TimeDimension -> [TimeDimension]
    function expandGranularTimeDimension(timeDimension) {
      const [dimension, granularity] = timeDimension;
      const makeTimeDimension = newGranularity => [dimension, newGranularity];

      const tds = [timeDimension];
      const updateTds = (...granularitys) => tds.push(...granularitys.map(makeTimeDimension))
      
      if (granularity === 'year') updateTds('hour', 'date', 'month');
      if (['month', 'week'].includes(granularity)) updateTds('hour', 'date');
      if (granularity === 'date') updateTds('hour');
      
      return tds;
    }

    function expandRangeTimeDimension(timeDimension) {
      const [dimension, dateRange, timezone] = timeDimension;
      const parsedRange = PreAggregations.parseDateRangeStringOrList(dateRange, timezone)
      console.log("parsedRange", {dateRange, parsedRange, timezone})
      return [dimension, parsedRange, timezone]
    }
    // [[TimeDimension]]
    const queryGranularTimeDimensionsList = transformedQuery.sortedGranularTimeDimensions.map(expandGranularTimeDimension);
    const queryRangeTimeDimensionsList = transformedQuery.sortedRangeTimeDimensions.map(expandRangeTimeDimension);

    console.log("queryTimeDimensionsList", {queryGranularTimeDimensionsList, queryRangeTimeDimensionsList})

    const canUsePreAggregationNotAdditive = (references) => {
        console.log("canUsePreAggregationNotAdditive", {references})
        return transformedQuery.hasNoTimeDimensionsWithoutGranularity &&
        transformedQuery.allFiltersWithinSelectedDimensions &&
        R.equals(references.sortedDimensions || references.dimensions, transformedQuery.sortedDimensions) &&
        (
            R.all(m => references.measures.indexOf(m) !== -1, transformedQuery.measures) ||
            R.all(m => references.measures.indexOf(m) !== -1, transformedQuery.leafMeasures)
        ) &&
        (
          transformedQuery.sortedGranularTimeDimensions &&
          R.equals(
            transformedQuery.sortedGranularTimeDimensions,
            references.sortedGranularTimeDimensions || PreAggregations.sortGranularTimeDimensions(references.timeDimensions)
          ) ||
          transformedQuery.sortedRangeTimeDimensions &&
          R.equals(
            transformedQuery.sortedRangeTimeDimensions,
            references.sortedRangeTimeDimensions || PreAggregations.sortRangeTimeDimensions(references.timeDimensions, transformedQuery.timezone)
          )
        );
    };

    const canUsePreAggregationLeafMeasureAdditive = (references) => {
        console.log("canUsePreAggregationLeafMeasureAdditive", {references})
        return R.all(
            d => (references.sortedDimensions || references.dimensions).indexOf(d) !== -1,
            transformedQuery.sortedDimensions
        ) &&
        R.all(m => references.measures.indexOf(m) !== -1, transformedQuery.leafMeasures) &&
        (
            (
                R.any(td => td.granularity, queryGranularTimeDimensionsList) &&
                R.allPass(
                  queryGranularTimeDimensionsList.map(tds => R.anyPass(tds.map(td => R.contains(td))))
              )(references.sortedGranularTimeDimensions || PreAggregations.sortGranularTimeDimensions(references.timeDimensions))
            ) ||
            (
                R.all(td => !td.granularity, queryRangeTimeDimensionsList) &&
                R.all(td => {
                  console.log("transformedQuery.timeDimensions", {td, queryRangeTimeDimensionsList})
                    return R.all(
                        queryRangeTd =>
                          R.equals(queryRangeTd[0], td[0]) && R.equals(queryRangeTd[1], td[1]),
                        queryRangeTimeDimensionsList
                    );
                }, references.sortedRangeTimeDimensions || PreAggregations.sortRangeTimeDimensions(references.timeDimensions, transformedQuery.timezone))
            )
        );
    };

    const canUsePreAggregationAdditive = (references) => {
        console.log("canUsePreAggregationAdditive", {references})
        if (references.timeDimensions){
          console.log("timeDimensions", {td: references.timeDimensions})
        }
        return R.all(
            d => (references.sortedDimensions || references.dimensions).indexOf(d) !== -1,
            transformedQuery.sortedDimensions
        ) &&
        (
            R.all(m => references.measures.indexOf(m) !== -1, transformedQuery.measures) ||
            R.all(m => references.measures.indexOf(m) !== -1, transformedQuery.leafMeasures)
        ) &&
        (
            (
                R.any(td => td.granularity, queryGranularTimeDimensionsList) &&
                R.allPass(
                  queryGranularTimeDimensionsList.map(tds => R.anyPass(tds.map(td => R.contains(td))))
              )(references.sortedGranularTimeDimensions || PreAggregations.sortGranularTimeDimensions(references.timeDimensions))
            ) ||
            (
                R.all(td => !td.granularity, queryRangeTimeDimensionsList) &&
                R.all(td => {
                  console.log("transformedQuery.timeDimensions", {td, queryRangeTimeDimensionsList})
                    return R.all(
                        queryRangeTd =>
                          // TODO - values don't match exactly if using EOD for second date
                          R.equals(queryRangeTd[0], td[0]) && R.equals(queryRangeTd[1], td[1]),
                        queryRangeTimeDimensionsList
                    );
                }, references.sortedRangeTimeDimensions || PreAggregations.sortRangeTimeDimensions(references.timeDimensions, transformedQuery.timezone))
            )
        );
    };

    let canUseFn;
    if (transformedQuery.isAdditive) {
      canUseFn = canUsePreAggregationAdditive;
    } else if (transformedQuery.leafMeasureAdditive) {
      canUseFn = canUsePreAggregationLeafMeasureAdditive;
    } else {
      canUseFn = canUsePreAggregationNotAdditive;
    }
    console.log("canUseFn", {canUseFn})
    if (refs) {
      return canUseFn(refs);
    } else {
      return canUseFn;
    }
  }

  static squashDimensions(query) {
    return R.pipe(R.uniq, R.sortBy(R.identity))(
      query.dimensions.concat(query.filters).map(d => d.dimension).concat(query.segments.map(s => s.segment))
    );
  }

  getCubeLattice(cube, preAggregationName, preAggregation) {
    throw new UserError('Auto rollups supported only in Enterprise version');
  }

  findPreAggregationForQuery() {
    if (!this.preAggregationForQuery) {
      const query = this.query;

      if (PreAggregations.hasCumulativeMeasures(query)) {
        return null;
      }

      const canUsePreAggregation = this.canUsePreAggregationFn(query);

      this.preAggregationForQuery = R.pipe(
        R.map(cube => {
          const preAggregations = this.query.cubeEvaluator.preAggregationsForCube(cube);
          console.log("findPreAggregationForQuery", {preAggregations})
          let rollupPreAggregations = R.pipe(
            R.toPairs,
            R.filter(([k, a]) => a.type === 'rollup'),
            R.filter(([k, aggregation]) => canUsePreAggregation(this.evaluateAllReferences(cube, aggregation))),
            R.map(([preAggregationName, preAggregation]) => ({ preAggregationName, preAggregation, cube })),
            R.sort((a, b) => {
              // sort so ones with matching dateRange is first
              console.log("sorting!", {a, b})
              if (a.preAggregation.dateRange){
                if (b.preAggregation.dateRange) {
                  return 0
                }
                return -1
              } else if (b.preAggregation.dateRange) {
                return 1
              }
              return -1
            })
          )(preAggregations);
          if (
            R.any(m => m.path() && m.path()[0] === cube, this.query.measures) ||
            !this.query.measures.length && !this.query.timeDimensions.length &&
            R.all(d => d.path() && d.path()[0] === cube, this.query.dimensions)
          ) {
            const autoRollupPreAggregations = R.pipe(
              R.toPairs,
              R.filter(([k, a]) => a.type === 'autoRollup'),
              R.map(([preAggregationName, preAggregation]) => {
                const cubeLattice = this.getCubeLattice(cube, preAggregationName, preAggregation);
                const optimalPreAggregation = cubeLattice.findOptimalPreAggregationFromLattice(this.query);
                return optimalPreAggregation && {
                  preAggregationName: preAggregationName + this.autoRollupNameSuffix(cube, optimalPreAggregation),
                  preAggregation: Object.assign(
                    optimalPreAggregation,
                    preAggregation
                  ),
                  cube
                };
              })
            )(preAggregations);
            rollupPreAggregations = rollupPreAggregations.concat(autoRollupPreAggregations);
          }
          console.log("returning rollupPreAggregations", {rollupPreAggregations})
          return rollupPreAggregations;
        }),
        R.unnest
      )(query.collectCubeNames())[0];
    }
    console.log("findPreAggregationForQuery final preagg", {p: this.preAggregationForQuery})
    return this.preAggregationForQuery;
  }

  static hasCumulativeMeasures(query) {
    const measures = (query.measures.concat(query.measureFilters));
    return R.pipe(
      R.map(m => query.collectLeafMeasures(() => query.traverseSymbol(m))),
      R.unnest,
      R.uniq,
      R.map(p => query.newMeasure(p)),
      R.any(m => m.isCumulative())
    )(measures);
  }

  castGranularity(granularity) {
    // TODO replace date granularity with day
    if (granularity === 'day') {
      return 'date';
    }
    return granularity;
  }

  castDateRange(dateRange) {
    // TODO
    return dateRange;
  }

  collectOriginalSqlPreAggregations(fn) {
    const preAggregations = [];
    const result = this.query.evaluateSymbolSqlWithContext(fn, { collectOriginalSqlPreAggregations: preAggregations });
    return { preAggregations, result };
  }

  rollupPreAggregationQuery(cube, aggregation) {
    const references = this.evaluateAllReferences(cube, aggregation);
    return this.query.newSubQuery({
      rowLimit: null,
      measures: references.measures,
      dimensions: references.dimensions,
      timeDimensions: this.mergePartitionTimeDimensions(references, aggregation.partitionTimeDimensions),
      preAggregationQuery: true,
      useOriginalSqlPreAggregationsInPreAggregation: aggregation.useOriginalSqlPreAggregations,
      collectOriginalSqlPreAggregations: this.query.safeEvaluateSymbolContext().collectOriginalSqlPreAggregations
    });
  }

  autoRollupPreAggregationQuery(cube, aggregation) {
    return this.query.newSubQuery({
      rowLimit: null,
      measures: aggregation.measures,
      dimensions: aggregation.dimensions,
      timeDimensions:
        this.mergePartitionTimeDimensions(aggregation, aggregation.partitionTimeDimensions),
      preAggregationQuery: true,
      useOriginalSqlPreAggregationsInPreAggregation: aggregation.useOriginalSqlPreAggregations,
      collectOriginalSqlPreAggregations: this.query.safeEvaluateSymbolContext().collectOriginalSqlPreAggregations
    });
  }

  mergePartitionTimeDimensions(aggregation, partitionTimeDimensions) {
    if (!partitionTimeDimensions) {
      return aggregation.timeDimensions;
    }
    return aggregation.timeDimensions.map(d => {
      const toMerge = partitionTimeDimensions.find(
        qd => qd.dimension === d.dimension
      );
      return toMerge ? Object.assign({}, d, { dateRange: toMerge.dateRange }) : d;
    });
  }

  autoRollupNameSuffix(cube, aggregation) {
    return '_' + aggregation.dimensions.concat(
      aggregation.timeDimensions.map(d => `${d.dimension}${d.granularity.substring(0, 1)}`)
    ).map(s => {
      const path = s.split('.');
      return `${path[0][0]}${path[1]}`;
    }).map(s => s.replace(/_/g, '')).join("_").replace(/[.]/g, '').toLowerCase();
  }

  evaluateAllReferences(cube, aggregation) {
    const timeDimensions = aggregation.timeDimensionReference ? [Object.assign({
        dimension: this.evaluateReferences(cube, aggregation.timeDimensionReference)
    },
      aggregation.granularity && {granularity: this.castGranularity(aggregation.granularity)},
      aggregation.dateRange && {dateRange: this.castDateRange(aggregation.dateRange)},
    )] : [];
    return {
      dimensions:
        (aggregation.dimensionReferences && this.evaluateReferences(cube, aggregation.dimensionReferences) || []).concat(
          aggregation.segmentReferences && this.evaluateReferences(cube, aggregation.segmentReferences) || []
        ),
      measures:
        aggregation.measureReferences && this.evaluateReferences(cube, aggregation.measureReferences) || [],
      timeDimensions
    };
  }

  evaluateReferences(cube, referencesFn) {
    return this.query.cubeEvaluator.evaluateReferences(cube, referencesFn);
  }

  rollupPreAggregation(preAggregationForQuery) {
    const table = preAggregationForQuery.preAggregation.partitionGranularity && this.query.timeDimensions.length ?
      this.partitionUnion(preAggregationForQuery) :
      this.query.preAggregationTableName(
        preAggregationForQuery.cube,
        preAggregationForQuery.preAggregationName
      );
    let segmentFilters = this.query.segments.map(s =>
      this.query.newFilter({ dimension: s.segment, operator: 'equals', values: [true] })
    );
    let filters = segmentFilters.concat(this.query.filters)
    if (!preAggregationForQuery.preAggregation.dateRange) {
      filters = filters.concat(this.query.timeDimensions.map(dimension => dimension.dateRange && ({
          filterToWhere: () => this.query.timeRangeFilter(
              this.query.dimensionSql(dimension),
              this.query.timeStampInClientTz(dimension.dateFromParam()),
              this.query.timeStampInClientTz(dimension.dateToParam())
          )
      }))).filter(f => !!f);
    }

    const renderedMeasureReference = R.pipe(
      R.map(path => {
        const measure = this.query.newMeasure(path);
        return [
          path,
          this.query.aggregateOnGroupedColumn(measure.measureDefinition(), measure.aliasName()) ||
          `sum(${measure.aliasName()})`
        ];
      }),
      R.fromPairs
    )(preAggregationForQuery.preAggregation.type === 'autoRollup' ?
      preAggregationForQuery.preAggregation.measures :
      this.evaluateAllReferences(preAggregationForQuery.cube, preAggregationForQuery.preAggregation).measures
    );

    const renderedDimensionReference = R.pipe(
      R.map(path => {
        const dimension = this.query.newDimension(path);
        return [
          path,
          this.query.aggregateOnGroupedColumn(dimension.dimensionDefinition(), dimension.aliasName()) ||
          dimension.aliasName()
        ];
      }),
      R.fromPairs
    )(preAggregationForQuery.preAggregation.type === 'autoRollup' ?
      preAggregationForQuery.preAggregation.dimensions :
      this.evaluateAllReferences(preAggregationForQuery.cube, preAggregationForQuery.preAggregation).dimensions
    );

    const renderedTimeDimensionsReference = R.pipe(
      R.map(path => {
        const timeDimension = this.query.newTimeDimension(path);
        return [
          path,
          this.query.aggregateOnGroupedColumn(timeDimension.dimensionDefinition(), timeDimension.aliasName()) ||
          timeDimension.aliasName()
        ];
      }),
      R.fromPairs
    )(preAggregationForQuery.preAggregation.type === 'autoRollup' ?
      preAggregationForQuery.preAggregation.timeDimensions :
      this.evaluateAllReferences(preAggregationForQuery.cube, preAggregationForQuery.preAggregation).timeDimensions
    );

    const renderedReference = Object.assign({}, renderedMeasureReference, renderedDimensionReference, renderedTimeDimensionsReference);

    const rollupGranularity = this.castGranularity(preAggregationForQuery.preAggregation.granularity);

    return this.query.evaluateSymbolSqlWithContext(
      () => `SELECT ${this.query.baseSelect()} FROM ${table} ${this.query.baseWhere(filters)}` +
        this.query.groupByClause() +
        this.query.baseHaving(this.query.measureFilters) +
        this.query.orderBy() +
        this.query.groupByDimensionLimit(),
      Object.assign(
        {
          renderedReference,
          rollupQuery: true
        },
        rollupGranularity && { rollupGranularity }
      )
    );
  }

  partitionUnion(preAggregationForQuery) {
    const { dimension, partitionDimension } = this.partitionDimension(preAggregationForQuery);

    const union = partitionDimension.timeSeries().map(range => {
      const preAggregation = this.addPartitionRangeTo(preAggregationForQuery, dimension, range);
      return this.preAggregationTableName(
        preAggregationForQuery.cube,
        preAggregationForQuery.preAggregationName,
        preAggregation.preAggregation
      );
    }).map(table => `SELECT * FROM ${table}`).join(" UNION ALL ");
    return `(${union}) as partition_union`;
  }
}

module.exports = PreAggregations;
