// Import mongoose library
const mongoose = require('mongoose');
const MongoClient = require('./base');
const CollectionName = "kaggle_movies_metadata_nested";

const mySchema = new mongoose.Schema({
    release_decade: {
        type: Number,
        index: true
    },
    movies: [{
        budget: Number,
        genres: [{
            id: Number,
            name: String
        }],
        id: Number,
        imdb_id: String,
        popularity: Number,
        revenue: Number,
        vote_average: Number
    }]
});

class KaggleMovieMetaDataNested extends MongoClient {
    constructor(modelName, schema) {
        super(modelName, schema);
        this.model.ensureIndexes()
    }

    // Update the upsert method to include LessonIndex and LessonSession
    async upsert(data) {
        try {
            // console.log(data)
            const filter = {
                release_decade: data.release_decade,
            };
            const update = { $set: data };
            const options = { upsert: true, new: true };

            const result = await this.model.findOneAndUpdate(filter, update, options);
            return result;
        } catch (error) {
            console.error('Error upserting course info:', error);
            throw error;
        }
    }
}

const model = new KaggleMovieMetaDataNested(CollectionName, mySchema);

module.exports = model;
