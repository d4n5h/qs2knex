# QS2Knex

Converts a QS string or parsed query object to knex query.

Gutted from Strapi and modified for general usage.

## Install

```bash
yarn add qs2knex

or

npm install qs2knex
```

## Usage

```javascript
const { applyQuery, applyParsed } = require('qs2knex');
const knex = require('knex')({ client: 'pg' });

// From string
const query = 'where[$and][0][$or][0][test]=1&where[$and][0][$or][1][test]=2&where[$and][1][$or][0][qwe]=3&where[$and][1][$or][1][asd]=4&orderBy[id]=asc&orderBy[name]=desc&select[0]=id&select[1]=name&limit=1&offset=0&groupBy[0]=id&groupBy[1]=name&having[id][$gt]=1&having[id][$notNull]=true&having[name][$in][0]=a&having[name][$in][1]=b&having[name][$notIn][0]=c&having[name][$notIn][1]=d&having[name][$between][0]=1&having[name][$between][1]=2&having[name][$notBetween][0]=3&having[name][$notBetween][1]=4&having[email][$null]=true&joins[0][alias]=user&joins[0][referencedTable]=users&joins[0][referencedColumn]=id&joins[0][rootColumn]=user_id&joins[0][on][id]=1&joins[0][orderBy][id]=asc';
const qb = knex('tableName');
applyQuery(qb, query);
console.log(qb.toString());

// From object
const obj = {
    where: {
        $and: [
            {
                $or: [
                    {
                        test: 1,
                    },
                    {
                        test: 2,
                    },
                ],
            },
            {
                $or: [
                    {
                        qwe: 3,
                    },
                    {
                        asd: 4,
                    },
                ],
            },
        ],
    },
    orderBy: {
        id: 'asc',
        name: 'desc',
    },
    select: ['id', 'name'],
    limit: 1,
    offset: 0,
    groupBy: ['id', 'name'],
    having: {
        id: {
            $gt: 1,
            $notNull: true,
        },
        name: {
            $in: ['a', 'b'],
            $notIn: ['c', 'd'],
            $between: [1, 2],
            $notBetween: [3, 4],
        },
        email: {
            $null: true,
        },
    },
    joins: [
        {
            alias: 'user',
            referencedTable: 'users',
            referencedColumn: 'id',
            rootColumn: 'user_id',
            on: {
                id: 1,
            },
            orderBy: {
                id: 'asc',
            },
        },
    ],
};

const qb2 = knex('tableName');

applyParsed(qb2, obj);
console.log(qb2.toString());
```
