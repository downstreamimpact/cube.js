const UserError = require('../compiler/UserError');
const PreAggregations = require('./PreAggregations')
const SmartLattice = require('./SmartLattice')

class SmartPreAggregations extends PreAggregations {
  getCubeLattice(cube, preAggregationName, preAggregation) {
      console.log("smart preaggregation cube lattice", {cube, preAggregationName, preAggregation})
      console.log("cubeLattice", { cache:this.cubeLatticeCache, lattices: this.cubeLattices } )
    return new SmartLattice(cube, preAggregation, this.cubeLatticeCache, this.cubeLattices)
  }
}

module.exports = SmartPreAggregations;
