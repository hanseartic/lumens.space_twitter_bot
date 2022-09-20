const {Keypair} = require("stellar-sdk");
const db = require("./db");
const yargs = require("yargs/yargs");
const { hideBin } = require("yargs/helpers");
const BigNumber = require("bignumber.js");
const exampleAddress = Keypair.random().publicKey();

const argv = yargs(hideBin(process.argv))
    .command('count', 'Count the cleans for a given address')
    .example('$0 count -a ' + exampleAddress.substring(0, 5) + '...' + exampleAddress.substring(51), 'count the cleans for the given address')
    .alias('a', 'address')
    .nargs('a', 1)
    .describe('a', 'lookup given address')

    .coerce('a', address => {
        try {
            return Keypair.fromPublicKey(address).publicKey()
        } catch (e) {
            throw new Error('Invalid address given');
        }
    })
    .demandCommand(1)
    .demandOption('a', 'You need to provide an address to lookup')
    .parse();


const { count } = db.database().queryFirstRowObject('SELECT count(*) as count FROM payments WHERE sender = ?', argv.address)
console.log("claimable balances cleaned by", argv.address.substring(0, 5) + '...' + argv.address.substring(51), new BigNumber(count).toString());
