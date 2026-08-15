"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const lodash_1 = __importDefault(require("lodash"));
const sequelize_1 = __importDefault(require("sequelize"));
const validator_1 = __importDefault(require("validator"));
const common_1 = require("@crowd/common");
const sequelizeRepository_1 = __importDefault(require("../sequelizeRepository"));
const { Op } = sequelize_1.default;
/**
 * Pass `db` connection object which has `Sequelize.Op`
 * @param {*} db
 * @returns object {parse}
 */
class QueryParser {
    constructor({ aggregators = {}, nestedFields = {}, manyToMany = {}, customOperators = {}, exportMode = false, withSegments = true, segmentsNullable = false, }, options) {
        this.options = options;
        this.aggregators = aggregators;
        this.nestedFields = nestedFields;
        this.whereOrHaving = 'where';
        if (this.aggregators && Object.keys(this.aggregators).length !== 0) {
            this.whereOrHaving = 'having';
        }
        this.manyToMany = manyToMany;
        this.customOperators = customOperators;
        this.exportMode = exportMode;
        this.withSegments = withSegments;
        this.segmentsNullable = segmentsNullable;
    }
    /**
     * If an invalid UUID is passed to a query, it throws an exception.
     * To hack this behaviour, if the uuid is invalid, it creates a new one,
     * that won't match any of the database.
     * If the uuid is invalid, brings no results.
     */
    static uuid(value) {
        let id = value;
        // If ID is invalid, sequelize throws an error.
        // For that not to happen, if the UUID is invalid, it sets
        // some random uuid
        if (!validator_1.default.isUUID(id)) {
            id = (0, common_1.generateUUIDv4)();
        }
        return id;
    }
    /**
     * Replaces operator (json object key) with Sequelize operator.
     * @param {JSON} json
     * @param {string} key
     * @param {Sequelize.op} op
     */
    static replaceKeyWithOperator(query, key, op) {
        const value = query[key];
        delete query[key];
        query[op] = value;
        return query;
    }
    /**
     * Replace a key with a complex operator. A complex operator is a function that acts on a key.
     * @param query query object
     * @param key key that will be replaced
     * @param complexOp function that will be called with the value of the key
     * @returns The query object with the key replaced by the result of the function
     */
    static replaceKeyWithComplexOperator(query, key, complexOp, args = {}) {
        const value = query[key];
        delete query[key];
        return {
            ...query,
            ...complexOp(value, args),
        };
    }
    /**
     * Replace a query[key] with a nested field
     * @param query Query object
     * @param key Key that marks the value to be replaced
     * @returns Query where query[key] is replaced with a nested field
     */
    replaceKeyWithNestedField(query, key) {
        const value = query[key];
        delete query[key];
        query[this.nestedFields[key]] = value;
        return query;
    }
    /**
     * Parse order by
     * @param orderBy Order by (string or string[]). If string: 'field_ASC,field2_DESC'. If array: ['field_ASC', 'field2_DESC']
     * @returns {Array<string[]>}
     */
    parseOrderBy(orderBy) {
        // If type is string, convert to list of strings
        if (typeof orderBy === 'string') {
            orderBy = orderBy.split(',').map((item) => item.trim());
        }
        return orderBy.map((item) => {
            // Replace nested attributes. For example:
            // if we had {sentiment: sentiment.score} in the nested keys,
            // we would replace [[sentiment], [DESC]] -> [[sentiment.score], [DESC]]
            const splitItem = item.split('_');
            if (this.nestedFields[splitItem[0]]) {
                splitItem[0] = this.nestedFields[splitItem[0]];
            }
            return splitItem;
        });
    }
    /**
     * Replace a key with an aggregator
     * @param query Query that we are parsing
     * @param key Key that we are parsing
     * @returns Query replaced by aggregator
     */
    replaceKeyWithAggregator(query, key) {
        // We will need 'having' instead of where for aggregators
        this.whereOrHaving = 'having';
        // Save value and delete
        const value = query[key];
        delete query[key];
        // The LHS of the  query will be the aggregator, instead of the key
        const left = this.aggregators[key];
        // The operator can be a proper operator, if we had for example
        // {activityCount: {gt: 10}} (the operator would be Op.gt)
        // Or it can be equal if we had
        // {platform: github} (then we would be picking Op.eq)
        let op = typeof value === 'object' ? QueryParser.operators[Object.keys(value)[0]] : Op.eq;
        // The RHS of the query will be the value, if we had
        // {platform: github} (then we would be picking github)
        // Or it can be the value of the object, if we had
        // {activityCount: {gt: 10}} (the value would be 10)
        let right = typeof value === 'object' ? value[Object.keys(value)[0]] : value;
        // handle textContains and jsonContains for literals
        if ((typeof value === 'object' && Object.keys(value)[0] === 'textContains') ||
            Object.keys(value)[0] === 'jsonContains') {
            op = Op.iLike;
            right = `%${right}%`;
        }
        // We wrap everything onto a where clause and we return
        let where = sequelize_1.default.where(left, op, right);
        // When we feed arrays directly in sequelize literals, it tries to cast
        // it to a postgres array.
        // This is not needed in `Op.in` queries. Simple lists are enough
        if (op === Op.in) {
            where = sequelize_1.default.where(left, op, sequelize_1.default.literal(`(${right.map((i) => `'${i}'`).toString()})`));
        }
        if (query[Op.and]) {
            query[Op.and].push(where);
        }
        else {
            query[Op.and] = [where];
        }
        return query;
    }
    /**
     *
     * @param query Query object
     * @param key Key that we are parsing
     * @returns
     */
    replaceWithManyToMany(query, key) {
        var _a;
        // Save value and delete
        const value = query[key];
        delete query[key];
        // The mapping comes from the manyToMany field for that key
        const mapping = this.manyToMany[key];
        // We construct the items to filter on.
        const items = value.reduce((acc, item, index) => {
            if (index === 0) {
                return `${acc} "${mapping.relationTable.name}"."${mapping.relationTable.to}"  = '${QueryParser.uuid(item)}'`;
            }
            return `${acc} OR "${mapping.relationTable.name}"."${mapping.relationTable.to}"  = '${QueryParser.uuid(item)}'`;
        }, '');
        const joinField = (_a = mapping.overrideJoinField) !== null && _a !== void 0 ? _a : 'id';
        // Find all the rows in the table that have the items we are filtering on
        const literal = sequelize_1.default.literal(`(SELECT "${mapping.table}"."${joinField}" FROM "${mapping.table}" INNER JOIN "${mapping.relationTable.name}" ON "${mapping.relationTable.name}"."${mapping.relationTable.from}" = "${mapping.table}"."${joinField}" WHERE ${items})`);
        // It coudl be that we have more than 1 many to many filter, so we could need to append.
        if (query[Op.and]) {
            query[Op.and].push(sequelize_1.default.where(sequelize_1.default.literal(`"${mapping.model}"."${joinField}"`), Op.in, literal));
        }
        else {
            query[Op.and] = [
                sequelize_1.default.where(sequelize_1.default.literal(`"${mapping.model}"."${joinField}"`), Op.in, literal),
            ];
        }
        return query;
    }
    /**
     * Iteratively replace json keys with Sequelize formated query operators.
     * @param {JSON} json next json
     */
    iterativeReplace(query) {
        Object.keys(query).forEach((key) => {
            if (this.nestedFields[key]) {
                // If the key should be replaced by a nested field we update the query and the key.
                // This is not part of the else if since we still want to do the other checks.
                query = this.replaceKeyWithNestedField(query, key);
                key = this.nestedFields[key];
            }
            if (this.aggregators[key]) {
                // If the key is one of the aggregators, replace by aggregator
                query = this.replaceKeyWithAggregator(query, key);
            }
            else if (key === 'id') {
                // When an ID is sent, we validate it.
                query[key] = QueryParser.uuid(query[key]);
            }
            else if (this.customOperators[key]) {
                // The complex operator could be substituting the key also.
                // For example, in member.platform, we are sent: {platform: jsonContains: 'github'}
                // and we need to replace the platform key with the function result.
                const complexOp = QueryParser.complexOperators[Object.keys(query[key])[0]];
                query = QueryParser.replaceKeyWithComplexOperator(query, key, complexOp, this.customOperators[key]);
            }
            else if (this.manyToMany[key]) {
                // If the key is a many to many field, construct the query
                query = this.replaceWithManyToMany(query, key);
            }
            else if (query[key] !== null && typeof query[key] === 'object') {
                // When the value of the key is an object
                // Find if the key is an operation in the operations dict
                const op = QueryParser.operators[key];
                if (op) {
                    // If we found an operation
                    // Replace the key (operator string) with the Sequelize operator
                    // Example: { "or": {...} } => { [Op.or]: {...} }
                    query = QueryParser.replaceKeyWithOperator(query, key, op);
                    // Recursively call the function with what is indisde the operator
                    query[op] = this.iterativeReplace(query[op]);
                }
                else {
                    // If we didn't find an operation, then it is a nested object
                    // Recursively call the function with the nested object
                    query[key] = this.iterativeReplace(query[key]);
                }
            }
            else if (key === 'model' && this.options.database[query[key]] != null) {
                // query['as'] = query[key].replace(/^./, char => char.toLowerCase());// /^\w/
                query.model = this.options.database[query[key]];
            }
            else {
                const op = QueryParser.operators[key];
                const complexOp = QueryParser.complexOperators[key];
                if (op)
                    query = QueryParser.replaceKeyWithOperator(query, key, op);
                else if (complexOp)
                    query = QueryParser.replaceKeyWithComplexOperator(query, key, complexOp);
                // The key is not an operation, but it could be that the value is (NULL, for example)
                else if (QueryParser.operators[query[key]] !== undefined)
                    if (query[key] === 'NULL' || query[key] === 'null')
                        query[key] = null;
                if (query[key] === 'NOT_NULL' || query[key] === 'not_null')
                    query[key] = { [Op.not]: null };
            }
        });
        return query;
    }
    /**
     * Parse and build Sequelize format query
     * @param {JSON} query
     * @returns {JSON} sequelize formatted DB query params JSON
     */
    parseQueryFields(query) {
        return this.iterativeReplace(query);
    }
    /**
     * Parse and build Sequelize format query
     * @param {JSON} query
     * @returns {JSON} sequelize formatted DB include params JSON
     */
    parseIncludeFields(query) {
        const json = query;
        this.iterativeReplace(json);
        return json;
    }
    /**
     * Pare a query into a sequelize query
     *
     * @param {QueryInput} query
     * @returns {QueryOutput} sequelize formatted DB query
     */
    parse(query = {
        filter: {},
        orderBy: undefined,
        limit: 0,
        offset: 0,
        include: false,
        fields: false,
    }) {
        // eslint-disable-next-line prefer-const
        let { filter, orderBy, limit, offset, include, fields } = query;
        const dbQuery = {
            where: {
                tenantId: sequelizeRepository_1.default.getCurrentTenant(this.options).id,
            },
            limit: QueryParser.defaultPageSize,
            offset: 0,
        };
        if (this.withSegments) {
            if (this.manyToMany.segments) {
                const segmentsQuery = this.replaceWithManyToMany({
                    segments: sequelizeRepository_1.default.getSegmentIds(this.options),
                }, 'segments');
                dbQuery.where = {
                    [Op.and]: [dbQuery.where, segmentsQuery],
                };
            }
            else if (this.segmentsNullable) {
                dbQuery.where.segmentId = {
                    [Op.or]: [
                        sequelizeRepository_1.default.getSegmentIds(this.options),
                        {
                            [Op.is]: null,
                        },
                    ],
                };
            }
            else {
                dbQuery.where.segmentId = sequelizeRepository_1.default.getSegmentIds(this.options);
            }
        }
        if (fields) {
            if (typeof fields === 'string') {
                fields = fields.split(',');
            }
            // assign fields array to .attributes
            if (fields && fields.length > 0)
                dbQuery.attributes = fields;
        }
        if (limit) {
            if (typeof limit === 'string') {
                limit = parseInt(limit, 10);
            }
            dbQuery.limit = this.exportMode
                ? 1000000000
                : Math.min(limit > 0 ? limit : QueryParser.defaultPageSize, QueryParser.maxPageSize);
        }
        if (offset) {
            if (typeof offset === 'string') {
                offset = parseInt(offset, 10);
            }
            dbQuery.offset = offset;
        }
        if (orderBy) {
            dbQuery.order = this.parseOrderBy(orderBy);
        }
        if (!lodash_1.default.isEmpty(filter)) {
            const parsed = this.parseQueryFields(filter);
            dbQuery[this.whereOrHaving] = { ...dbQuery[this.whereOrHaving], ...parsed };
        }
        if (include) {
            dbQuery.include = this.parseIncludeFields(query.include);
        }
        return dbQuery;
    }
}
QueryParser.maxPageSize = 200;
QueryParser.defaultPageSize = 10;
QueryParser.operators = {
    gt: Op.gt,
    gte: Op.gte,
    lt: Op.lt,
    lte: Op.lte,
    ne: Op.ne,
    eq: Op.eq,
    not: Op.not,
    // like: Op.like, // LIKE '%hat'
    // notLike: Op.notLike, // NOT LIKE '%hat'
    like: Op.iLike, // ILIKE '%hat' (case insensitive) (PG only)
    notLike: Op.notILike, // NOT ILIKE '%hat'  (PG only)
    regexp: Op.regexp, // REGEXP/~ '^[h|a|t]' (MySQL/PG only)
    notRegexp: Op.notRegexp, // NOT REGEXP/!~ '^[h|a|t]' (MySQL/PG only)
    // iRegexp: Op.iRegexp,            // ~* '^[h|a|t]' (PG only)
    // notIRegexp: Op.notIRegexp       // !~* '^[h|a|t]' (PG only)
    and: Op.and, // AND (a = 5)
    or: Op.or, // (a = 5 OR a = 6)
    between: Op.between, // BETWEEN 6 AND 10
    notBetween: Op.notBetween, // NOT BETWEEN 11 AND 15
    in: Op.in, // IN [1, 2]
    notIn: Op.notIn, // NOT IN [1, 2]
    overlap: Op.overlap, // && [1, 2] (PG array overlap operator)
    NULL: null,
    contains: Op.contains, // @> [1, 2] (PG array contains operator)
    // contained: Op.contained,        // <@ [1, 2] (PG array contained by operator)
    // col: Op.col                     // = "user"."organization_id", with dialect specific column identifiers, PG in this example
    // any: Op.any                     // ANY ARRAY[2, 3]::INTEGER (PG only)
};
QueryParser.complexOperators = {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    textContains: (value, _args = {}) => {
        const result = {
            [Op.iLike]: `%${value}%`,
        };
        return result;
    },
    jsonContains: (value, args) => {
        const where = sequelize_1.default.where(sequelize_1.default.literal(`CAST("${args.model}"."${args.column}" AS TEXT)`), {
            [sequelize_1.default.Op.like]: `%${Object.values(value)[0]}%`.toLowerCase(),
        });
        return { [Op.and]: where };
    },
};
exports.default = QueryParser;
//# sourceMappingURL=queryParser.js.map