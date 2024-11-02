// Import mongoose library
const mongoose = require('mongoose');
const MongoClient = require('./base');
const CollectionName = "kaggle_movies_metadata_optimized_v1";

const mySchema = new mongoose.Schema({
    budget: Number,
    genres: [{
        id: Number,
        name: String
    }],
    id: Number,
    imdb_id: String,
    popularity: Number,
    release_date: Date,
    revenue: Number,
    vote_average: Number,
});
// indexing: 
//compounding indexes
mySchema.index({ budget: 1, release_date: 1 }); 
//unique index
mySchema.index({ id: 1 }, { unique: true }); 

class KaggleMovieMetaData extends MongoClient {
    constructor(modelName, schema) {
        super(modelName, schema);
        this.model.ensureIndexes()
    }

    // Update the upsert method to include LessonIndex and LessonSession
    async upsert(data) {
        try {
            // console.log(data)
            const filter = {
                id: data.id,
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

const model = new KaggleMovieMetaData(CollectionName, mySchema);

module.exports = model;
