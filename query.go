// Copyright (C) xooooooox

package sea

import (
	"errors"
	"fmt"
	"reflect"
)

type Inquirer interface {
	Cols(...string) Inquirer
	Table(string) Inquirer
	Alias(string) Inquirer
	Join(...string) Inquirer
	Where(string, ...interface{}) Inquirer
	Group(...string) Inquirer
	Having(string, ...interface{}) Inquirer
	Asc(string) Inquirer
	Desc(string) Inquirer
	Page(uint64) Inquirer
	Limit(uint64) Inquirer
	Get(interface{}) error
}

type Inquiry struct {
	// column name
	cols []string
	// table name
	table string
	// alias name
	alias string
	// join name
	join string
	// where name
	where string
	// group name
	group string
	// having
	having string
	// order name
	order string
	// page page
	page uint64
	// limit limit
	limit uint64
	// sql
	sql string
	// args
	args []interface{}
}

func Qy(table ...string) Inquirer {
	q := &Inquiry{}
	// select * from person,dept where person.did = dept.did;
	for _, v := range table {
		if q.table == "" {
			q.table = fmt.Sprintf("%s", Flutter(v))
		} else {
			q.table = fmt.Sprintf("%s, %s", q.table, Flutter(v))
		}
	}
	return q
}

func (q *Inquiry) Cols(cols ...string) Inquirer {
	length := len(cols)
	if length == 0 {
		return q
	}
	q.cols = []string{}
	q.cols = append(q.cols, cols...)
	return q
}

func (q *Inquiry) Table(table string) Inquirer {
	q.table = Flutter(table)
	return q
}

func (q *Inquiry) Alias(alias string) Inquirer {
	q.alias = Flutter(alias)
	return q
}

func (q *Inquiry) Join(join ...string) Inquirer {
	for _, v := range join {
		v = FlutterSentence(v)
		if q.join == "" {
			q.join = v
			continue
		}
		q.join = fmt.Sprintf("%s %s", q.join, v)
	}
	return q
}

func (q *Inquiry) Where(where string, args ...interface{}) Inquirer {
	where = FlutterSentence(where)
	if q.where == "" {
		q.where = where
	} else {
		q.where = fmt.Sprintf("%s AND %s", q.where, where)
	}
	q.args = append(q.args, args...)
	return q
}

func (q *Inquiry) Group(group ...string) Inquirer {
	length := len(group)
	for i := 0; i < length; i++ {
		if q.group == "" {
			q.group = Flutter(group[i])
			continue
		}
		q.group = fmt.Sprintf("%s, %s", q.group, Flutter(group[i]))
	}
	return q
}

func (q *Inquiry) Having(having string, args ...interface{}) Inquirer {
	having = FlutterSentence(having)
	if q.having == "" {
		q.having = having
	} else {
		q.having = fmt.Sprintf("%s AND %s", q.having, having)
	}
	q.args = append(q.args, args...)
	return q
}

func (q *Inquiry) Asc(order string) Inquirer {
	if q.order == "" {
		q.order = fmt.Sprintf("%s ASC", Flutter(order))
		return q
	}
	q.order = fmt.Sprintf("%s, %s ASC", q.order, Flutter(order))
	return q
}

func (q *Inquiry) Desc(order string) Inquirer {
	if q.order == "" {
		q.order = fmt.Sprintf("%s DESC", Flutter(order))
		return q
	}
	q.order = fmt.Sprintf("%s, %s DESC", q.order, Flutter(order))
	return q
}

func (q *Inquiry) Page(page uint64) Inquirer {
	q.page = page
	return q
}

func (q *Inquiry) Limit(limit uint64) Inquirer {
	q.limit = limit
	return q
}

func (q *Inquiry) Get(get interface{}) error {
	// check columns first
	cols := ""
	if len(q.cols) == 0 {
		cols = "*"
	} else {
		for _, v := range q.cols {
			v = Flutter(v)
			if cols == "" {
				cols = v
				continue
			}
			cols = fmt.Sprintf("%s, %s", cols, v)
		}
	}
	// table name is not set
	if q.table == "" {
		t := reflect.TypeOf(get)
		kind := t.Kind()
		if kind != reflect.Ptr {
			return errors.New("require pointer parameter")
		}
		t = t.Elem()
		kind = t.Kind()
		switch kind {
		case reflect.Struct:
			q.table = Flutter(PascalToUnderline(t.Name()))
		case reflect.Slice:
			t = t.Elem()
			kind = t.Kind()
			if kind == reflect.Ptr {
				t = t.Elem()
			}
			q.table = Flutter(PascalToUnderline(t.Name()))
		default:
			return errors.New("unsupported data type")
		}
	}
	q.sql = fmt.Sprintf("SELECT %s FROM %s", cols, q.table)
	if q.alias != "" {
		q.sql = fmt.Sprintf("%s %s", q.sql, q.alias)
	}
	if q.join != "" {
		q.sql = fmt.Sprintf("%s %s", q.sql, q.join)
	}
	if q.where != "" {
		q.sql = fmt.Sprintf("%s WHERE ( %s )", q.sql, q.where)
	}
	if q.group != "" {
		q.sql = fmt.Sprintf("%s GROUP BY %s", q.sql, q.group)
		if q.having != "" {
			q.sql = fmt.Sprintf("%s HAVING ( %s )", q.sql, q.having)
		}
	}
	if q.order != "" {
		q.sql = fmt.Sprintf("%s ORDER BY %s", q.sql, q.order)
	}
	// set limit x,y; if not set limit, use 1000 as default value
	if q.limit == 0 {
		q.limit = 1000
	}
	if q.page == 0 {
		q.sql = fmt.Sprintf("%s LIMIT %d", q.sql, q.limit)
	} else {
		q.sql = fmt.Sprintf("%s LIMIT %d,%d", q.sql, (q.page-1)*q.limit, q.limit)
	}
	return Get(get, q.sql, q.args...)
}
