const mongoose = require('mongoose');
var Schema = mongoose.Schema;
const endpoint = "mongodb://127.0.0.1:27017/movie_budget";

class MongoClient {
	constructor(modelName, schema) {
		this.modelName = modelName;
		this.schema = schema;
		this.connect()
	}

	async connect() {
		const options = {
			useNewUrlParser: true,
			useUnifiedTopology: true,
			serverSelectionTimeoutMS: 5000
		};

		const connection = mongoose.createConnection(
			endpoint,
			options
		);

		//connect model
		var SchemaModel = new Schema(this.schema, { collection: this.modelName, versionKey: false });
		this.model = connection.model(this.modelName, SchemaModel);
		this.schema = SchemaModel
	}

	// doing CRUD
	async create(data) {
		try {
			const result = await this.model.create(data);
			return result;
		} catch (error) {
			console.error('Error inserting data:', error);
			throw error;
		}
	}
	async read(query = {}) {
		try {
			const result = await this.model.find(query);
			return result;
		} catch (error) {
			console.error('Error retrieving data:', error);
			throw error;
		}
	}

	async update(query, updateData) {
		try {
			const result = await this.model.updateMany(query, updateData);
			return result;
		} catch (error) {
			console.error('Error updating data:', error);
			throw error;
		}
	}

	async delete(query) {
		try {
			const result = await this.model.deleteMany(query);
			return result;
		} catch (error) {
			console.error('Error deleting data:', error);
			throw error;
		}
	}

	async findOneAndUpdate(query, updateData, options = { new: true }) {
		try {
			const result = await this.model.findOneAndUpdate(query, updateData, options);
			return result;
		} catch (error) {
			console.error('Error finding and updating document:', error);
			throw error;
		}
	}
}

exports = module.exports = MongoClient;