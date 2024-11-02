const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');
const wikiMoviesMetaDataNested = require('./models/kaggle_model_nested');

const csvFilePath = path.join(__dirname, '..', 'movie_review', 'movies_metadata.csv');

const processCSV = () => {
    console.log("Processing CSV file...");
    const readStream = fs.createReadStream(csvFilePath);
    const csvParser = csv();

    readStream.pipe(csvParser);
    
    let moviesByDecade = {};
    csvParser.on('data', (row) => {
        const processedRow = processRow(row);
        if (processedRow) {
            const decade = processedRow.release_decade;
            if (!moviesByDecade[decade]) {
                moviesByDecade[decade] = [];
            }
            moviesByDecade[decade].push(processedRow);
        }
    });

    csvParser.on('end', async () => {
        console.log('CSV file processing completed');
        console.log('Updating MongoDB');
        
        for (const [decade, movies] of Object.entries(moviesByDecade)) {
            try {
                await wikiMoviesMetaDataNested.upsert({
                    release_decade: parseInt(decade),
                    movies: movies
                });
                console.log(`Processed decade: ${decade}`);
            } catch (error) {
                console.error(`Error processing decade ${decade}: ${error.message}`);
                process.exit(1);
            }
        }

        console.log('Finished');
        process.exit(0);
    });
};

const processRow = (row) => {
    if (row.adult !== 'False' && row.adult !== 'True') {
        console.log(`Skipping row with invalid 'adult' value: ${row.adult}`);
        return null;
    }

    const parseJsonField = (field) => {
        if (!field) return null;
        const attributesToReplace = ['name', 'id'];
        attributesToReplace.forEach(attr => {
            const regex = new RegExp(`'${attr}'`, 'g');
            field = field.replace(regex, `"${attr}"`);
        });
        field = field.replace(/: '/g, ': "').replace(/',/g, '",').replace(/'}/g, '"}').replace(/ None/g, ' null').replace('\\','\\\\');
        return field;
    };

    row.genres = JSON.parse(parseJsonField(row.genres) || '[]');
    row.budget = Number(row.budget || 0);
    row.id = Number(row.id || 0);
    row.popularity = Number(row.popularity || 0);
    row.revenue = Number(row.revenue || 0);
    row.vote_average = Number(row.vote_average || 0);
    row.vote_count = Number(row.vote_count || 0);

    if (row.release_date) {
        const year = new Date(row.release_date).getFullYear();
        row.release_decade = Math.floor(year / 10) * 10;
    } else {
        row.release_decade = null;
    }

    return row;
};

(async function main() {
    processCSV();
})();
