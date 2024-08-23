const { MongoClient } = require('mongodb');
const dotenv = require("dotenv")

dotenv.config()

let dbInstance = null;

async function getDbInstance() {
    if (dbInstance) {
        return dbInstance; 
    }

    const client = new MongoClient(process.env.MONGODB_URI);

    try {
        await client.connect();
        const database = client.db('RQ_Analytics'); 

        dbInstance = database; 
        return dbInstance;
    } catch (error) {
        console.error("Failed to connect to the database", error);
        throw error; 
    }
}

module.exports = { getDbInstance };
