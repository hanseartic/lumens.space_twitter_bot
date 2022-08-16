const BigNumber = require("bignumber.js");


const sumReducer = (prev, current) => new BigNumber(current).plus(prev);
module.exports = {
    sumReducer
};
