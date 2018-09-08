var express = require('express');
var request = require('request');
var router = express.Router();

const apiHostname = 'localhost';
const apiPort = 10483;

router.get('/', function(req, res, next) {
    var keyword = req.query.keyword;
    var method = req.query.method;
    var page = parseInt(req.query.page) || 1;

    if (keyword === undefined || method === undefined) {
        res.redirect('/');
    } else {
        keyword = keyword.trim();
        method = method.trim().toLowerCase();

        if (keyword.length === 0 || method.length === 0) {
            res.redirect('/');
        } else if (method !== 'lucene' && method !== 'mixer' && method !== 'mixerpr') {
            res.redirect('/');
        } else {
            request({
                url: 'http://' + apiHostname + ':' + apiPort + '/query',
                qs: { method: method, keyword: keyword, page: page },
                json: true
            }, function (err, _res, body) {
                if (body === undefined || !body.hasOwnProperty('error')) {
                    if (body === undefined) {
                        res.render('error', {error: err});
                    } else {
                        res.render('query', {
                            title: keyword + ' - WikiSearch',
                            keyword: keyword,
                            method: method,
                            hits: -1,
                            error: body
                        })
                    }
                } else if (body.error === true) {
                    res.render('query', {
                        title: keyword + ' - WikiSearch',
                        keyword: keyword,
                        method: method,
                        hits: -1,
                        error: body.data
                    })
                } else {
                    res.render('query', {
                        title: keyword + ' - WikiSearch',
                        keyword: keyword,
                        method: method,
                        pageNo: body.data.pageNo,
                        pageUrlSuffix: '/query?method=' + method + '&keyword=' + keyword + '&page=',
                        totalPages: body.data.totalPages,
                        hits: body.data.hits,
                        pages: body.data.pages,
                        time: body.data.elapsedTime / 1000.0
                    });
                }
            });
        }
    }
});

module.exports = router;
