const UserError = require('../compiler/UserError');

class NoAuthCubeLattice {
  constructor(cube, preAggregation, cubeLatticeCache, cubeLattices) {
    this.cube = cube;
    this.preAggregation = preAggregation;
    this.cubeLatticeCache = cubeLatticeCache;
    this.cubeLattices = cubeLattices;
    console.log("SmartLattice", {cube, preAggregation, cubeLatticeCache, cubeLattices})
  }

  findOptimalPreAggregationFromLattice(query) {
      console.log("query", {query})
    throw new UserError('Auto rollups are only supported with Enterprise version or by implementing yourself by overriding the cubeLatticeFactory setting');
  }
}

module.exports = NoAuthCubeLattice;
