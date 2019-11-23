
class SmartLattice {
  constructor(cube, preAggregation, cubeLatticeCache, cubeLattices) {
    this.cube = cube;
    this.preAggregation = preAggregation;
    this.cubeLatticeCache = cubeLatticeCache;
    this.cubeLattices = cubeLattices || {};
    console.log("SmartLattice", {cube, preAggregation, cubeLatticeCache, cubeLattices})
  }

  findOptimalPreAggregationFromLattice(query) {
      console.log("query", {query})
      // return {
      //     preAggregationName: 'myauto',
      //     preAggregation: {
      //         type: 'rollup',
      //         measureReferences: query.measures,
      //         dimensionReferences: query.dimensions,
      //         timeDimensionReference: query.timeDimension,
      //         granularity: 'day',
      //         partitionGranularity: 'month'
      //     },
      //     cube: this.cube
      // }
      // return {
      //     type: 'rollup',
      //     measureReferences: query.measures,
      //     dimensionReferences: query.dimensions,
      //     timeDimensionReference: query.timeDimension,
      //     granularity: 'day',
      //     partitionGranularity: 'month'
      // }
      // TODO Implement logic
      // https://sookocheff.com/post/databases/implementing-data-cubes-efficiently/
      // S = {top view};
      // for i=1 to k do
      //     select view v not in S such that B(v,S) is maximized
      //     S = S union {v}
      // end
      // https://statsbot.co/blog/introducing-querying-history-constrained-algorithm-for-data-cube-lattice-calculation/
      // S = {queryUnion QueryHistory};
      // for i = 1 to k do begin
      //   Unions = QueryHistory
      //   MaxV = select that view v in QueryHistory and not in S such that B(v,S) is maximized;
      //   for v in Unions do begin
      //     for u in Unions do begin
      //       If B(MaxV, S) < B(queryUnion {u, v}, S) then
      //         MaxV = queryUnion {u, v}
      //         Unions = Unions union {MaxV}
      //       end;
      //     end;
      //   end;
      //
      //   S = S union {MaxV};
      // end;
      // resulting S is the querying history constrained greedy selection;
      return {
          type: 'rollup',
          measures: query.measures,
          dimensions: query.dimensions,
          timeDimensions: [query.timeDimension],
          granularity: 'day',
      }
  }
    // throw new UserError('Auto rollups supported where I say they are supported in cubee');
      // SmartLattice { cube: 'visitors',
      //   preAggregation: { type: 'autoRollup', maxPreAggregations: 20 },
      //   cubeLatticeCache: undefined,
      //   cubeLattices: {} }
      // query { query:
      //    SmartPostgresQuery {
      //      compilers:
      //       { joinGraph: [Object],
      //         cubeEvaluator: [Object],
      //         compiler: [Object] },
      //      cubeEvaluator:
      //       CubeEvaluator {
      //         symbols: [Object],
      //         builtCubes: [Object],
      //         cubeDefinitions: [Object],
      //         cubeList: [Array],
      //         cubeValidator: [Object],
      //         evaluatedCubes: [Object],
      //         primaryKeys: [Object],
      //         resolveSymbolsCallContext: undefined,
      //         byFileName: [Object] },
      //      joinGraph:
      //       JoinGraph {
      //         cubeValidator: [Object],
      //         cubeEvaluator: [Object],
      //         nodes: [Object],
      //         edges: [Object],
      //         undirectedNodes: [Object],
      //         graph: [Object],
      //         cachedConnectedComponents: [Object] },
      //      options:
      //       { measures: [Array],
      //         dimensions: [Array],
      //         timezone: 'America/Los_Angeles',
      //         preAggregationsSchema: '',
      //         timeDimensions: [],
      //         order: [] },
      //      orderHashToString: [Function: bound orderHashToString],
      //      defaultOrder: [Function: bound defaultOrder],
      //      contextSymbols: { userContext: {} },
      //      paramAllocator: PostgresParamAllocator { params: [] },
      //      timezone: 'America/Los_Angeles',
      //      rowLimit: undefined,
      //      offset: undefined,
      //      preAggregations:
      //       SmartPreAggregations {
      //         query: [Circular],
      //         historyQueries: [],
      //         cubeLatticeCache: undefined,
      //         cubeLattices: {} },
      //      measures: [ [Object] ],
      //      dimensions: [ [Object] ],
      //      segments: [],
      //      order: [ [Object] ],
      //      filters: [],
      //      measureFilters: [],
      //      timeDimensions: [],
      //      allFilters: [],
      //      evaluateSymbolContext: undefined,
      //      collectedCubeNames: [ 'visitors' ],
      //      join: { joins: [], root: 'visitors', multiplicationFactor: [Object] },
      //      cubeAliasPrefix: undefined,
      //      preAggregationsSchemaOption: '',
      //      externalQueryClass: undefined,
      //      ungrouped: undefined } }
}

module.exports = SmartLattice;
