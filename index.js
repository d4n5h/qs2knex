const KnexBuilder = require('knex/lib/query/querybuilder');
const KnexRaw = require('knex/lib/raw');
const qs = require('qs');

// Utils
const isKnexQuery = value => value instanceof KnexBuilder || value instanceof KnexRaw;
const fieldLowerFn = qb => qb.client.config.client === 'postgres' ? 'LOWER(CAST(?? AS VARCHAR))' : 'LOWER(??)';
const castArray = (...args) => args.reduce((acc, arg) => acc.concat(arg), []);
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

    switch (operator) {
        case '$not': {
            if (isWhere) {
                qb.whereNot((qb) => applyWhereToColumn(qb, column, value));
            } else {
                qb.havingNot((qb) => applyHavingToColumn(qb, column, value));
            }

            break;
        }
        case '$in': {
            if (isWhere) {
                qb.whereIn(column, isKnexQuery(value) ? value : castArray(value));
            } else {
                qb.havingIn(column, isKnexQuery(value) ? value : castArray(value));
            }

            break;
        }
        case '$notIn': {
            if (isWhere) {
                qb.whereNotIn(column, isKnexQuery(value) ? value : castArray(value));
            } else {
                qb.havingNotIn(column, isKnexQuery(value) ? value : castArray(value));
            }

            break;
        }
        case '$eq': {
            if (isWhere) {
                if (value === null) {
                    qb.whereNull(column);
                    break;
                }

                qb.where(column, value);
            } else {
                if (value === null) {
                    qb.havingNull(column);
                    break;
                }

                qb.having(column, value);
            }

            break;
        }
        case '$eqi': {
            if (isWhere) {
                if (value === null) {
                    qb.whereNull(column);
                    break;
                }
                qb.whereRaw(`${fieldLowerFn(qb)} LIKE LOWER(?)`, [column, `${value}`]);
            } else {
                if (value === null) {
                    qb.havingNull(column);
                    break;
                }
                qb.havingRaw(`${fieldLowerFn(qb)} LIKE LOWER(?)`, [column, `${value}`]);
            }

            break;
        }
        case '$ne': {
            if (isWhere) {
                if (value === null) {
                    qb.whereNotNull(column);
                    break;
                }

                qb.where(column, '<>', value);
            } else {
                if (value === null) {
                    qb.havingNotNull(column);
                    break;
                }

                qb.having(column, '<>', value);
            }

            break;
        }
        case '$nei': {
            if (isWhere) {
                if (value === null) {
                    qb.whereNotNull(column);
                    break;
                }

                qb.whereRaw(`${fieldLowerFn(qb)} NOT LIKE LOWER(?)`, [column, `${value}`]);

            } else {
                if (value === null) {
                    qb.havingNotNull(column);
                    break;
                }
                qb.havingRaw(`${fieldLowerFn(qb)} NOT LIKE LOWER(?)`, [column, `${value}`]);
            }

            break;
        }
        case '$gt': {
            if (isWhere) {
                qb.where(column, '>', value);
            } else {
                qb.having(column, '>', value);
            }

            break;
        }
        case '$gte': {
            if (isWhere) {
                qb.where(column, '>=', value);
            } else {
                qb.having(column, '>=', value);
            }

            break;
        }
        case '$lt': {
            if (isWhere) {
                qb.where(column, '<', value);
            } else {
                qb.having(column, '<', value);
            }

            break;
        }
        case '$lte': {
            if (isWhere) {
                qb.where(column, '<=', value);
            } else {
                qb.having(column, '<=', value);
            }

            break;
        }
        case '$null': {
            if (isWhere) {
                if (value) {
                    qb.whereNull(column);
                } else {
                    qb.whereNotNull(column);
                }
            } else {
                if (value) {
                    qb.havingNull(column);
                } else {
                    qb.havingNotNull(column);
                }
            }

            break;
        }
        case '$notNull': {
            if (isWhere) {
                if (value) {
                    qb.whereNotNull(column);
                } else {
                    qb.whereNull(column);
                }
            } else {
                if (value) {
                    qb.havingNotNull(column);
                } else {
                    qb.havingNull(column);
                }
            }

            break;
        }
        case '$between': {
            if (isWhere) {
                qb.whereBetween(column, value);
            } else {
                qb.havingBetween(column, value);
            }

            break;
        }
        case '$notBetween': {
            if (isWhere) {
                qb.whereNotBetween(column, value);
            } else {
                qb.havingNotBetween(column, value);
            }

            break;
        }
        case '$startsWith': {
            if (isWhere) {
                qb.where(column, 'like', `${value}%`);
            } else {
                qb.having(column, 'like', `${value}%`);
            }

            break;
        }
        case '$startsWithi': {
            if (isWhere) {
                qb.whereRaw(`${fieldLowerFn(qb)} LIKE LOWER(?)`, [column, `${value}%`]);
            } else {
                qb.havingRaw(`${fieldLowerFn(qb)} LIKE LOWER(?)`, [column, `${value}%`]);
            }

            break;
        }
        case '$endsWith': {
            if (isWhere) {
                qb.where(column, 'like', `%${value}`);
            } else {
                qb.having(column, 'like', `%${value}`);
            }

            break;
        }
        case '$endsWithi': {
            if (isWhere) {
                qb.whereRaw(`${fieldLowerFn(qb)} LIKE LOWER(?)`, [column, `%${value}`]);
            } else {
                qb.havingRaw(`${fieldLowerFn(qb)} LIKE LOWER(?)`, [column, `%${value}`]);
            }

            break;
        }
        case '$contains': {
            if (isWhere) {
                qb.where(column, 'like', `%${value}%`);
            } else {
                qb.having(column, 'like', `%${value}%`);
            }

            break;
        }
        case '$notContains': {
            if (isWhere) {
                qb.whereNot(column, 'like', `%${value}%`);
            } else {
                qb.havingNot(column, 'like', `%${value}%`);
            }

            break;
        }
        case '$containsi': {
            if (isWhere) {
                qb.whereRaw(`${fieldLowerFn(qb)} LIKE LOWER(?)`, [column, `%${value}%`]);
            } else {
                qb.havingRaw(`${fieldLowerFn(qb)} LIKE LOWER(?)`, [column, `%${value}%`]);
            }

            break;
        }
        case '$notContainsi': {
            if (isWhere) {
                qb.whereRaw(`${fieldLowerFn(qb)} NOT LIKE LOWER(?)`, [column, `%${value}%`]);
            } else {
                qb.havingRaw(`${fieldLowerFn(qb)} NOT LIKE LOWER(?)`, [column, `%${value}%`]);
            }

            break;
        }
        case '$jsonSupersetOf': {
            if (isWhere) {
                qb.whereJsonSupersetOf(column, value);
            } else {
                qb.havingJsonSupersetOf(column, value);
            }
            break;
        }
        case '$jsonNotSupersetOf': {
            if (isWhere) {
                qb.whereJsonNotSupersetOf(column, value);
            } else {
                qb.havingJsonNotSupersetOf(column, value);
            }
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
 * @param {any} where Where
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
 * @param {any} orderBy Order by
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
 * @param {any} select Select
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
 * @param {any} limit Limit
 * @returns {Object} Knex query builder instance
 */
const applyLimit = (qb, limit) => {
    if (limit) qb.limit(limit);
    return qb;
};

/**
 * Apply offset to query builder
 * @param {Object} qb Knex query builder instance
 * @param {any} offset Offset
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
 * @param {any} column Column name
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
 * @param {any} having Having
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
 * @param {any} join Join
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

        if (on) {
            Object.entries(on).forEach(([key, value]) => {
                inner.onVal(`${alias}.${key}`, value);
            });
        }
    });

    if (orderBy) {
        Object.entries(orderBy).forEach(([column, direction]) => {
            qb.orderBy(`${alias}.${column}`, direction);
        });
    }

    return qb;
};

/**
 * Apply joins to query builder
 * @param {Object} qb Knex query builder instance
 * @param {any} joins Joins
 * @returns {Object} Knex query builder instance
 */
const applyJoins = (qb, joins) => joins.forEach((join) => applyJoin(qb, join));

/**
 * Apply parsed qs object to query builder
 * @param {Object} qb Knex query builder instance
 * @param {any} query Query
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