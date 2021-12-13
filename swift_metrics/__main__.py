from .prom import stats_doc
import sys
import wsgiref.simple_server

if 'server' in sys.argv or 'serve' in sys.argv:
    def app(env, start_response):
        if env['PATH_INFO'] != '/metrics':
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return [b'Not Found']
        body = stats_doc().encode('utf-8')
        start_response('200 OK', [
            ('Content-Length', str(len(body))),
            ('Content-Type', 'text/plain'),
        ])
        return [body]

    with wsgiref.simple_server.make_server('', 8000, app) as httpd:
        httpd.serve_forever()

else:
    print(stats_doc(), end='')
