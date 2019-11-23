const UserError = require('../compiler/UserError');

class SmartLattice {
  constructor(cube, preAggregation, cubeLatticeCache, cubeLattices) {
    this.cube = cube;
    this.preAggregation = preAggregation;
    this.cubeLatticeCache = cubeLatticeCache;
    this.cubeLattices = cubeLattices;
    console.log("SmartLattice", {cube, preAggregation, cubeLatticeCache, cubeLattices})
  }

  findOptimalPreAggregationFromLattice(query) {
      console.log("query", {query})
    throw new UserError('Auto rollups supported where I say they are supported in cubee');
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
}

module.exports = SmartLattice;
