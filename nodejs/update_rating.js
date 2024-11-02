const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');
const kaggleMoviesMetaData = require('./models/kaggle_model_metadata_optimized');

const csvFilePath = path.join(__dirname, '..', 'movie_review', 'ratings.csv');
const linksFilePath = path.join(__dirname, '..', 'movie_review', 'links.csv');

const createTmdbToMovieIdMap = async () => {
    return new Promise((resolve, reject) => {
        const movieIdToTmdbMap = {};
        fs.createReadStream(linksFilePath)
            .pipe(csv())
            .on('data', (row) => {
                const movieId = parseInt(row.movieId);
                const tmdbId = parseInt(row.tmdbId);
                if (!isNaN(tmdbId)) {
                    movieIdToTmdbMap[movieId] = tmdbId;
                }
            })
            .on('end', () => {
                console.log("Finished creating movieId to tmdbId map");
                resolve(movieIdToTmdbMap);
            })
            .on('error', (error) => {
                reject(error);
            });
    });
};

const processRatings = () => {
    console.log("Processing ratings CSV file...");
    const readStream = fs.createReadStream(csvFilePath);
    const csvParser = csv();

    readStream.pipe(csvParser);
    
    let ratingData = {};

    csvParser.on('data', (row) => {
        const movieId = parseInt(row.movieId);
        const rating = parseFloat(row.rating);

        if (!ratingData[movieId]) {
            ratingData[movieId] = { sum: 0, count: 0 };
        }
        ratingData[movieId].sum += rating;
        ratingData[movieId].count += 1;
    });

    csvParser.on('end', async () => {
        console.log('CSV file processing completed');
        console.log('Updating MongoDB');

        const movieIdToTmdbMap = await createTmdbToMovieIdMap();
        for (const [movieId, data] of Object.entries(ratingData)) {
            const rating_average = data.sum / data.count;
            const rating_count = data.count;

            try {
                let err = await kaggleMoviesMetaData.update(
                    { id: movieIdToTmdbMap[parseInt(movieId)] },
                    { $set: { rating_average, rating_count } }
                );
                console.log(`Updated movie ${movieId}`);
            } catch (error) {
                console.error(`Error updating movie ${movieId}:`, error.message);
            }
        }
        console.log('Finished');
        process.exit(0);

    });
};

(async function main() {
    processRatings();
})();
