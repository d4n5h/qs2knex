const KnexBuilder = require('knex/lib/query/querybuilder');
const KnexRaw = require('knex/lib/raw');
const qs = require('qs');
const { isArray, castArray, isPlainObject } = require('lodash/fp');

const isKnexQuery = (value) => value instanceof KnexBuilder || value instanceof KnexRaw;

const fieldLowerFn = (qb) => {
    if (qb.client.config.client === 'postgres') return 'LOWER(CAST(?? AS VARCHAR))';
    return 'LOWER(??)';
};

const applyOperator = (type, qb, column, operator, value) => {
    if (Array.isArray(value) && !['$in', '$notIn', '$between'].includes(operator)) {
        if (type == 'where') {
            return qb.where((subQB) => {
                value.forEach((subValue) =>
                    subQB.orWhere((innerQB) => {
                        applyOperator(type, innerQB, column, operator, subValue);
                    })
                );
            });
        } else if (type == 'having') {
            return qb.having((subQB) => {
                value.forEach((subValue) =>
                    subQB.orHaving((innerQB) => {
                        applyOperator(type, innerQB, column, operator, subValue);
                    })
                );
            });
        }
    }

    switch (operator) {
        case '$not': {
            if (type == 'where') {
                qb.whereNot((qb) => applyWhereToColumn(qb, column, value));
            } else if (type == 'having') {
                qb.havingNot((qb) => applyHavingToColumn(qb, column, value));
            }

            break;
        }
        case '$in': {
            if (type == 'where') {
                qb.whereIn(column, isKnexQuery(value) ? value : castArray(value));
            } else if (type == 'having') {
                qb.havingIn(column, isKnexQuery(value) ? value : castArray(value));
            }

            break;
        }
        case '$notIn': {
            if (type == 'where') {
                qb.whereNotIn(column, isKnexQuery(value) ? value : castArray(value));
            } else if (type == 'having') {
                qb.havingNotIn(column, isKnexQuery(value) ? value : castArray(value));
            }

            break;
        }
        case '$eq': {
            if (type == 'where') {
                if (value === null) {
                    qb.whereNull(column);
                    break;
                }

                qb.where(column, value);
            } else if (type == 'having') {
                if (value === null) {
                    qb.havingNull(column);
                    break;
                }

                qb.having(column, value);
            }

            break;
        }
        case '$eqi': {
            if (type == 'where') {
                if (value === null) {
                    qb.whereNull(column);
                    break;
                }
                qb.whereRaw(`${fieldLowerFn(qb)} LIKE LOWER(?)`, [column, `${value}`]);
            } else if (type == 'having') {
                if (value === null) {
                    qb.havingNull(column);
                    break;
                }
                qb.havingRaw(`${fieldLowerFn(qb)} LIKE LOWER(?)`, [column, `${value}`]);
            }

            break;
        }
        case '$ne': {
            if (type == 'where') {
                if (value === null) {
                    qb.whereNotNull(column);
                    break;
                }

                qb.where(column, '<>', value);
            } else if (type == 'having') {
                if (value === null) {
                    qb.havingNotNull(column);
                    break;
                }

                qb.having(column, '<>', value);
            }

            break;
        }
        case '$nei': {
            if (type == 'where') {
                if (value === null) {
                    qb.whereNotNull(column);
                    break;
                }

                qb.whereRaw(`${fieldLowerFn(qb)} NOT LIKE LOWER(?)`, [column, `${value}`]);

            } else if (type == 'having') {
                if (value === null) {
                    qb.havingNotNull(column);
                    break;
                }
                qb.havingRaw(`${fieldLowerFn(qb)} NOT LIKE LOWER(?)`, [column, `${value}`]);
            }

            break;
        }
        case '$gt': {
            if (type == 'where') {
                qb.where(column, '>', value);
            } else if (type == 'having') {
                qb.having(column, '>', value);
            }

            break;
        }
        case '$gte': {
            if (type == 'where') {
                qb.where(column, '>=', value);
            } else if (type == 'having') {
                qb.having(column, '>=', value);
            }

            break;
        }
        case '$lt': {
            if (type == 'where') {
                qb.where(column, '<', value);
            } else if (type == 'having') {
                qb.having(column, '<', value);
            }

            break;
        }
        case '$lte': {
            if (type == 'where') {
                qb.where(column, '<=', value);
            } else if (type == 'having') {
                qb.having(column, '<=', value);
            }

            break;
        }
        case '$null': {
            if (type == 'where') {
                if (value) {
                    qb.whereNull(column);
                } else {
                    qb.whereNotNull(column);
                }
            } else if (type == 'having') {
                if (value) {
                    qb.havingNull(column);
                } else {
                    qb.havingNotNull(column);
                }
            }

            break;
        }
        case '$notNull': {
            if (type == 'where') {
                if (value) {
                    qb.whereNotNull(column);
                } else {
                    qb.whereNull(column);
                }
            } else if (type == 'having') {
                if (value) {
                    qb.havingNotNull(column);
                } else {
                    qb.havingNull(column);
                }
            }

            break;
        }
        case '$between': {
            if (type == 'where') {
                qb.whereBetween(column, value);
            } else if (type == 'having') {
                qb.havingBetween(column, value);
            }

            break;
        }
        case '$notBetween': {
            if (type == 'where') {
                qb.whereNotBetween(column, value);
            } else if (type == 'having') {
                qb.havingNotBetween(column, value);
            }

            break;
        }
        case '$startsWith': {
            if (type == 'where') {
                qb.where(column, 'like', `${value}%`);
            } else if (type == 'having') {
                qb.having(column, 'like', `${value}%`);
            }

            break;
        }
        case '$startsWithi': {
            if (type == 'where') {
                qb.whereRaw(`${fieldLowerFn(qb)} LIKE LOWER(?)`, [column, `${value}%`]);
            } else if (type == 'having') {
                qb.havingRaw(`${fieldLowerFn(qb)} LIKE LOWER(?)`, [column, `${value}%`]);
            }

            break;
        }
        case '$endsWith': {
            if (type == 'where') {
                qb.where(column, 'like', `%${value}`);
            } else if (type == 'having') {
                qb.having(column, 'like', `%${value}`);
            }

            break;
        }
        case '$endsWithi': {
            if (type == 'where') {
                qb.whereRaw(`${fieldLowerFn(qb)} LIKE LOWER(?)`, [column, `%${value}`]);
            } else if (type == 'having') {
                qb.havingRaw(`${fieldLowerFn(qb)} LIKE LOWER(?)`, [column, `%${value}`]);
            }

            break;
        }
        case '$contains': {
            if (type == 'where') {
                qb.where(column, 'like', `%${value}%`);
            } else if (type == 'having') {
                qb.having(column, 'like', `%${value}%`);
            }

            break;
        }
        case '$notContains': {
            if (type == 'where') {
                qb.whereNot(column, 'like', `%${value}%`);
            } else if (type == 'having') {
                qb.havingNot(column, 'like', `%${value}%`);
            }

            break;
        }
        case '$containsi': {
            if (type == 'where') {
                qb.whereRaw(`${fieldLowerFn(qb)} LIKE LOWER(?)`, [column, `%${value}%`]);
            } else if (type == 'having') {
                qb.havingRaw(`${fieldLowerFn(qb)} LIKE LOWER(?)`, [column, `%${value}%`]);
            }

            break;
        }
        case '$notContainsi': {
            if (type == 'where') {
                qb.whereRaw(`${fieldLowerFn(qb)} NOT LIKE LOWER(?)`, [column, `%${value}%`]);
            } else if (type == 'having') {
                qb.havingRaw(`${fieldLowerFn(qb)} NOT LIKE LOWER(?)`, [column, `%${value}%`]);
            }

            break;
        }
        case '$jsonSupersetOf': {
            if (type == 'where') {
                qb.whereJsonSupersetOf(column, value);
            } else if (type == 'having') {
                qb.havingJsonSupersetOf(column, value);
            }
            break;
        }
        case '$jsonNotSupersetOf': {
            if (type == 'where') {
                qb.whereJsonNotSupersetOf(column, value);
            } else if (type == 'having') {
                qb.havingJsonNotSupersetOf(column, value);
            }
            break;
        }
        default: {
            throw new Error(`Undefined attribute level operator ${operator} (${column})`);
        }
    }
}

const applyWhereToColumn = (qb, column, columnWhere) => {
    if (!isPlainObject(columnWhere)) {
        if (Array.isArray(columnWhere)) return qb.whereIn(column, columnWhere);

        return qb.where(column, columnWhere);
    }

    Object.keys(columnWhere).forEach((operator) => {
        const value = columnWhere[operator];

        applyOperator('where', qb, column, operator, value);
    });
};

const applyWhere = (qb, where) => {
    if (!isArray(where) && !isPlainObject(where)) throw new Error('Where must be an array or an object');

    if (isArray(where)) return qb.where((subQB) => where.forEach((subWhere) => applyWhere(subQB, subWhere)));

    for (const key in where) {
        const value = where[key];

        if (key === '$and') return qb.where((subQB) => value.forEach((v) => applyWhere(subQB, v)))

        if (key === '$or') {
            return qb.where((subQB) => {
                value.forEach((v) => subQB.orWhere((inner) => applyWhere(inner, v)));
            });
        }

        if (key === '$not') return qb.whereNot((qb) => applyWhere(qb, value));

        applyWhereToColumn(qb, key, value);
    }

    return qb;
};

const applyOrderBy = (qb, orderBy) => {
    qb.orderBy(orderBy);

    return qb;
};

const applySelect = (qb, select) => {
    if (!Array.isArray(select) || !typeof select == 'string') throw new Error('Select must be an array or a string');
    qb.select(select);

    return qb;
};

const applyLimit = (qb, limit) => {
    if (limit) qb.limit(limit);

    return qb;
};

const applyOffset = (qb, offset) => {
    if (offset) qb.offset(offset);

    return qb;
};

const applyGroupBy = (qb, groupBy) => {
    if (!Array.isArray(groupBy) || !typeof groupBy == 'string') throw new Error('GroupBy must be an array or a string');
    qb.groupBy(groupBy);

    return qb;
}

const applyHavingToColumn = (qb, column, columnHaving) => {
    if (!isPlainObject(columnHaving)) {
        if (Array.isArray(columnHaving)) return qb.havingIn(column, columnHaving);

        return qb.having(column, columnHaving);
    }

    Object.keys(columnHaving).forEach((operator) => {
        const value = columnHaving[operator];
        applyOperator('having', qb, column, operator, value);
    });
};

const applyHaving = (qb, having) => {
    if (!isArray(having) && !isPlainObject(having)) throw new Error('Having must be an array or an object');

    if (isArray(having)) return qb.having((subQB) => having.forEach((subHaving) => applyHaving(subQB, subHaving)));

    for (const key in having) {
        const value = having[key];

        if (key === '$and') return qb.having((subQB) => value.forEach((v) => applyHaving(subQB, v)))

        if (key === '$or') {
            return qb.where((subQB) => {
                value.forEach((v) => subQB.orWhere((inner) => applyHaving(inner, v)));
            });
        }

        if (key === '$not') return qb.whereNot((qb) => applyHaving(qb, value));

        applyHavingToColumn(qb, key, value);
    }

    return qb;
};

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

        if (on) for (const key of Object.keys(on)) inner.onVal(`${alias}.${key}`, on[key]);
    });

    if (orderBy) {
        Object.keys(orderBy).forEach((column) => {
            const direction = orderBy[column];
            qb.orderBy(`${alias}.${column}`, direction);
        });
    }

    return qb;
};

const applyJoins = (qb, joins) => joins.forEach((join) => applyJoin(qb, join));

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