const base = require('./webpack.base.conf')

module.exports = Object.assign({}, base, {
    devServer: {
        port: 9010,
        proxy: [{
            context: ['/v1'],
            changeOrigin: true,
            secure: false,
            target: 'http://localhost:8083'
        }],
        disableHostCheck: true
    },
    mode: 'development'
})
