const KnexBuilder = require('knex/lib/query/querybuilder');
const KnexRaw = require('knex/lib/raw');
const qs = require('qs');

// Utils
const isKnexQuery = value => value instanceof KnexBuilder || value instanceof KnexRaw;
const fieldLowerFn = qb => qb.client.config.client === 'postgres' ? 'LOWER(CAST(?? AS VARCHAR))' : 'LOWER(??)';
const castArray = (...args) => args.flat();
const getTag = value => value == null ? value === undefined ? '[object Undefined]' : '[object Null]' : Object.prototype.toString.call(value);
const isObjectLike = value => typeof value === 'object' && value !== null;

const isPlainObject = (value) => {
    if (!isObjectLike(value) || getTag(value) !== '[object Object]') return false;
    if (Object.getPrototypeOf(value) === null) return true;

    let proto = value;

    while (Object.getPrototypeOf(proto) !== null) proto = Object.getPrototypeOf(proto);

    return Object.getPrototypeOf(value) === proto;
}

/**
 * Apply operator to query builder
 * @param {Enumerator} type "where" or "having"
 * @param {Object} qb Knex query builder instance
 * @param {any} column Column name
 * @param {any} operator Operator
 * @param {any} value Value
 * @returns {Object} Knex query builder instance
 */
const applyOperator = (type, qb, column, operator, value) => {
    const isWhere = type === 'where';
    const orType = isWhere ? 'orWhere' : 'orHaving';

    if (Array.isArray(value) && !['$in', '$notIn', '$between'].includes(operator)) {
        return qb[type]((subQB) => {
            value.forEach((subValue) =>
                subQB[orType]((innerQB) => {
                    applyOperator(type, innerQB, column, operator, subValue);
                })
            );
        });
    }

    const applyToColumn = isWhere ? applyWhereToColumn : applyHavingToColumn;

    switch (operator) {
        case '$not': {
            qb[type + 'Not']((qb) => applyToColumn(qb, column, value));
            break;
        }
        case '$in': {
            qb[type + 'In'](column, isKnexQuery(value) ? value : castArray(value));
            break;
        }
        case '$notIn': {
            qb[type + 'NotIn'](column, isKnexQuery(value) ? value : castArray(value));
            break;
        }
        case '$eq': {
            if (value === null) {
                qb[type + 'Null'](column);
            } else {
                qb[type](column, value);
            }
            break;
        }
        case '$eqi': {
            if (value === null) {
                qb[type + 'Null'](column);
            } else {
                qb[type + 'Raw'](`${fieldLowerFn(qb)} LIKE LOWER(?)`, [column, `${value}`]);
            }
            break;
        }
        case '$ne': {
            if (value === null) {
                qb[type + 'NotNull'](column);
            } else {
                qb[type](column, '<>', value);
            }
            break;
        }
        case '$nei': {
            if (value === null) {
                qb[type + 'NotNull'](column);
            } else {
                qb[type + 'Raw'](`${fieldLowerFn(qb)} NOT LIKE LOWER(?)`, [column, `${value}`]);
            }
            break;
        }
        case '$gt': {
            qb[type](column, '>', value);
            break;
        }
        case '$gte': {
            qb[type](column, '>=', value);
            break;
        }
        case '$lt': {
            qb[type](column, '<', value);
            break;
        }
        case '$lte': {
            qb[type](column, '<=', value);
            break;
        }
        case '$null': {
            if (value) {
                qb[type + 'Null'](column);
            } else {
                qb[type + 'NotNull'](column);
            }
            break;
        }
        case '$notNull': {
            if (value) {
                qb[type + 'NotNull'](column);
            } else {
                qb[type + 'Null'](column);
            }
            break;
        }
        case '$between': {
            qb[type + 'Between'](column, value);
            break;
        }
        case '$notBetween': {
            qb[type + 'NotBetween'](column, value);
            break;
        }
        case '$startsWith': {
            qb[type](column, 'like', `${value}%`);
            break;
        }
        case '$startsWithi': {
            qb[type + 'Raw'](`${fieldLowerFn(qb)} LIKE LOWER(?)`, [column, `${value}%`]);
            break;
        }
        case '$endsWith': {
            qb[type](column, 'like', `%${value}`);
            break;
        }
        case '$endsWithi': {
            qb[type + 'Raw'](`${fieldLowerFn(qb)} LIKE LOWER(?)`, [column, `%${value}`]);
            break;
        }
        case '$contains': {
            qb[type](column, 'like', `%${value}%`);
            break;
        }
        case '$notContains': {
            qb[type + 'Not'](column, 'like', `%${value}%`);
            break;
        }
        case '$containsi': {
            qb[type + 'Raw'](`${fieldLowerFn(qb)} LIKE LOWER(?)`, [column, `%${value}%`]);
            break;
        }
        case '$notContainsi': {
            qb[type + 'Raw'](`${fieldLowerFn(qb)} NOT LIKE LOWER(?)`, [column, `%${value}%`]);
            break;
        }
        case '$jsonSupersetOf': {
            qb[type + 'JsonSupersetOf'](column, value);
            break;
        }
        case '$jsonNotSupersetOf': {
            qb[type + 'JsonNotSupersetOf'](column, value);
            break;
        }
        default: throw new Error(`Undefined attribute level operator ${operator} (${column})`);
    }
}

/**
 * Apply where to column
 * @param {Object} qb Knex query builder instance
 * @param {any} column Column name
 * @param {any} columnWhere Column where
 * @returns {Object} Knex query builder instance
 */
const applyWhereToColumn = (qb, column, columnWhere) => {
    if (!isPlainObject(columnWhere)) {
        if (Array.isArray(columnWhere)) {
            return qb.whereIn(column, columnWhere);
        } else {
            return qb.where(column, columnWhere);
        }
    }

    Object.entries(columnWhere).forEach(([operator, value]) => {
        applyOperator('where', qb, column, operator, value);
    });
};

/**
 * Apply where to query builder
 * @param {Object} qb Knex query builder instance
 * @param {Object} where Where
 * @returns {Object} Knex query builder instance
 */
const applyWhere = (qb, where) => {
    if (!Array.isArray(where) && !isPlainObject(where)) throw new Error('Where must be an array or an object');

    if (Array.isArray(where)) {
        where.forEach((subWhere) => applyWhere(qb, subWhere));
    } else {
        for (const key in where) {
            const value = where[key];

            switch (key) {
                case '$and':
                    value.forEach((v) => applyWhere(qb, v));
                    break;
                case '$or':
                    qb.where((subQB) => {
                        value.forEach((v) => subQB.orWhere((inner) => applyWhere(inner, v)));
                    });
                    break;
                case '$not':
                    qb.whereNot((subQB) => applyWhere(subQB, value));
                    break;
                default:
                    applyWhereToColumn(qb, key, value);
                    break;
            }
        }
    }

    return qb;
};

/**
 * Apply order by to query builder
 * @param {Object} qb Knex query builder instance
 * @param {Object} orderBy Order by
 * @returns {Object} Knex query builder instance
 */
const applyOrderBy = (qb, orderBy) => {
    if (!isPlainObject(orderBy)) throw new Error('OrderBy must be an object');

    Object.entries(orderBy).forEach(([key, value]) => {
        if (value === 'asc' || value === 'desc') {
            qb.orderBy(key, value);
        } else {
            throw new Error(`Invalid order direction ${value}`);
        }
    });

    return qb;
};

/**
 * Apply select to query builder
 * @param {Object} qb Knex query builder instance
 * @param {String | Array} select Select
 * @returns {Object} Knex query builder instance
 */
const applySelect = (qb, select) => {
    if (!Array.isArray(select) || !typeof select == 'string') throw new Error('Select must be an array or a string');
    qb.select(select);

    return qb;
};

/**
 * Apply limit to query builder
 * @param {Object} qb Knex query builder instance
 * @param {Number} limit Limit
 * @returns {Object} Knex query builder instance
 */
const applyLimit = (qb, limit) => {
    if (limit) qb.limit(limit);
    return qb;
};

/**
 * Apply offset to query builder
 * @param {Object} qb Knex query builder instance
 * @param {Number} offset Offset
 * @returns {Object} Knex query builder instance
 */
const applyOffset = (qb, offset) => {
    if (offset) qb.offset(offset);
    return qb;
};

/**
 * Apply group by to query builder
 * @param {Object} qb Knex query builder instance
 * @param {any} groupBy Group by
 * @returns {Object} Knex query builder instance
 */
const applyGroupBy = (qb, groupBy) => {
    if (!Array.isArray(groupBy) || !typeof groupBy == 'string') throw new Error('GroupBy must be an array or a string');
    qb.groupBy(groupBy);

    return qb;
}

/**
 * Apply having to column
 * @param {Object} qb Knex query builder instance
 * @param {String} column Column name
 * @param {any} columnHaving Column having
 * @returns {Object} Knex query builder instance
 */
const applyHavingToColumn = (qb, column, columnHaving) => {
    if (!isPlainObject(columnHaving)) {
        if (Array.isArray(columnHaving)) {
            return qb.havingIn(column, columnHaving);
        } else {
            return qb.having(column, columnHaving);
        }
    }
    Object.entries(columnHaving).forEach(([operator, value]) => {
        applyOperator('having', qb, column, operator, value);
    });
};

/**
 * Apply having to query builder
 * @param {Object} qb Knex query builder instance
 * @param {Object} having Having
 * @returns {Object} Knex query builder instance
 */
const applyHaving = (qb, having) => {
    if (!Array.isArray(having) && !isPlainObject(having)) throw new Error('Having must be an array or an object');

    if (Array.isArray(having)) return qb.having((subQB) => having.forEach((subHaving) => applyHaving(subQB, subHaving)));

    for (const key in having) {
        const value = having[key];

        if (key === '$and') return qb.having((subQB) => value.forEach((v) => applyHaving(subQB, v)))

        if (key === '$or') return qb.where(subQB => value.forEach((v) => subQB.orWhere((inner) => applyHaving(inner, v))));

        if (key === '$not') return qb.whereNot((qb) => applyHaving(qb, value));

        applyHavingToColumn(qb, key, value);
    }

    return qb;
};

/**
 * Apply join to query builder
 * @param {Object} qb Knex query builder instance
 * @param {Object} join Join
 * @returns {Object} Knex query builder instance
 */
const applyJoin = (qb, join) => {
    const {
        method = 'leftJoin',
        alias,
        referencedTable,
        referencedColumn,
        rootColumn,
        rootTable = qb._single.table,
        on,
        orderBy,
    } = join;

    qb[method](`${referencedTable} as ${alias}`, (inner) => {
        inner.on(`${rootTable}.${rootColumn}`, `${alias}.${referencedColumn}`);

        if (on) Object.entries(on).forEach(([key, value]) => inner.onVal(`${alias}.${key}`, value));
    });

    if (orderBy) Object.entries(orderBy).forEach(([column, direction]) => qb.orderBy(`${alias}.${column}`, direction));

    return qb;
};

/**
 * Apply joins to query builder
 * @param {Object} qb Knex query builder instance
 * @param {Array} joins Joins
 * @returns {Object} Knex query builder instance
 */
const applyJoins = (qb, joins) => joins.forEach((join) => applyJoin(qb, join));

/**
 * Apply parsed qs object to query builder
 * @param {Object} qb Knex query builder instance
 * @param {Object} query Query
 * @returns {Object} Knex query builder instance
 * @example applyParsed(qb, { select: ['id','name], where: { name: 'John' }, limit: 10, offset: 20})
 */
const applyParsed = (qb, query) => {
    if (query?.select) applySelect(qb, query.select);
    if (query?.joins) applyJoins(qb, query.joins);
    if (query?.having) applyHaving(qb, query.having);
    if (query?.where) applyWhere(qb, query.where);
    if (query?.groupBy) applyGroupBy(qb, query.groupBy);
    if (query?.orderBy) applyOrderBy(qb, query.orderBy);
    if (query?.offset) applyOffset(qb, query.offset);
    if (query?.limit) applyLimit(qb, query.limit);

    return qb;
};

/**
 * Apply qs string to query builder
 * @param {Object} qb Knex query builder instance
 * @param {any} query Query string
 * @param {any} options qs options
 * @returns {Object} Knex query builder instance
 * @example applyQuery(qb, 'select=id&where[name]=John&limit=10&offset=20')
*/
const applyQuery = (qb, query, options) => {
    applyParsed(qb, qs.parse(query, options));
    return qb;
}

module.exports = {
    applyQuery,
    applyParsed,
    applyWhere,
    applyOrderBy,
    applySelect,
    applyLimit,
    applyOffset,
    applyGroupBy,
    applyHaving,
    applyJoins,
    applyJoin
}