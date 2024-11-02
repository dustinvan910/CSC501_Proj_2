const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');
const wikiMoviesMetaData = require('./models/kaggle_model_metadata_optimized');

const csvFilePath = path.join(__dirname, '..', 'movie_review', 'movies_metadata.csv');

const processCSV = () => {
    console.log("Processing CSV file...");
    const readStream = fs.createReadStream(csvFilePath);
    const csvParser = csv();

    readStream.pipe(csvParser);
    
    let movieList = []
    csvParser.on('data', (row) => {
        movieList.push(row)
    })

    csvParser.on('end', async () => {
        console.log('CSV file processing completed');
        console.log('Update MongoDB')
        let cnt = 0
        console.log(movieList.length)
        for (let row of movieList) {
            try {
                cnt++
                if (row.adult !== 'False' && row.adult !== 'True') {
                    console.log(row.adult)
                    continue
                }
                
                console.log(cnt,movieList.length)
                // Parse JSON strings
                const parseJsonField = (field) => {
                    if (!field) return null;
                    // Define an array of attributes to replace
                    const attributesToReplace = ['name', 'id'];

                    // Replace single quotes with double quotes for specified attributes
                    attributesToReplace.forEach(attr => {
                        const regex = new RegExp(`'${attr}'`, 'g');
                        field = field.replace(regex, `"${attr}"`);
                    });
                    
                    // Replace remaining single quotes with double quotes
                    field = field.replace(/: '/g, ': "').replace(/',/g, '",').replace(/'}/g, '"}').replace(/ None/g, ' null').replace('\\','\\\\');
                    
                    return field;
                };
                
                // Update fields in row
                row.genres = JSON.parse(parseJsonField(row.genres) || '[]');

                // Convert types
                row.budget = Number(row.budget || 0);
                row.id = Number(row.id || 0);
                row.popularity = Number(row.popularity || 0);
                row.revenue = Number(row.revenue || 0);
                row.vote_average = Number(row.vote_average || 0);
                row.vote_count = Number(row.vote_count || 0);

                // Convert release_date to decade
                if (row.release_date) {
                    const year = new Date(row.release_date).getFullYear();
                    row.release_decade = Math.floor(year / 10) * 10;
                } else {
                    row.release_decade = null;
                }
                
                await wikiMoviesMetaData.upsert(row);
                // console.log(`Processed: ${row.title}`);
            } catch (error) {
                console.error(`Error processing row: ${error.message}`);
                console.error(`Problematic row:`, JSON.stringify(row, null, 2));
                process.exit(1);
            }
        }

        console.log('Finished');
        process.exit(0);
        
    });
};

(async function main() {
    processCSV();
})()

