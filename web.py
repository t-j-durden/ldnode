import asyncio
import threading
from asyncio import AbstractEventLoop
from asyncio import Future
from rdflib.plugins.sparql.processor import SPARQLResult
from sanic import Sanic
from sanic.response import *
from sanic import response
from pybars import Compiler

# from rdfnode import LdNode

http = Sanic("rdf_node")
compiler = Compiler()


class http_session:
    node: 'LdNode' = None  # TODO: there must be a better way to do this!!
    http_loop: AbstractEventLoop = None
    http_task: Future = None


def run_http_server(loop, host, port):
    asyncio.set_event_loop(loop)
    http_server = http.create_server(host=host, port=port, return_asyncio_server=True)
    http_session.http_task = asyncio.ensure_future(http_server)
    loop.run_forever()
    print('http thread stopped')


def start_http_server(node, host, port):
    http_session.node = node
    loop = asyncio.new_event_loop()
    http_session.http_loop = loop
    http_thread = threading.Thread(target=run_http_server, args=(loop, host, port))
    http_thread.start()


def stop_http_server():
    print('stopping http server')
    http_session.http_task.cancel()  # TODO: need to fix 'Task was destroyed but it is pending' message
    http_session.http_loop.stop()
    print('http server stopped')


def build_result_template(result: SPARQLResult):
    headers = [{'header': str(h)} for h in result.vars]
    rows = []
    i = 0
    for row in result:
        i = i + 1
        cells = [{'cell': row[h].value} for h in result.vars]
        row = {'row': {
            'line': i,
            'cells': cells
        }}
        rows.append(row)
    context = {
        'headers': headers,
        'rows': rows
    }
    return context


def get_content_type(request):
    content_type = 'html'
    if 'content-type' in request.headers:
        content_type = request.headers['content-type']
    if 'content-type' in request.args:
        content_type = request.args['content-type'][0]
    return content_type


@http.route("/")
async def web_home(request):
    content_type = get_content_type(request)
    # TODO: use templates here
    if content_type == 'json':
        return json({'hi', 'word'})
    else:
        return html('hello!')


@http.route('/publisher/<publisher>')
async def web_data(request, publisher):
    processors = http_session.node.processors
    for k in http_session.node.processors.keys():
        if str(k).endswith(publisher):
            publisher = processors[k]
            return await publisher.get_data(response)



@http.route('/query/<query>')
async def web_query(request, query):
    # todo: make json a template
    # todo: move templates to RDF
    args = {}
    for arg in request.args:
        args[arg] = request.args[arg][0]
    rows = http_session.node.store.query(query, args)
    ct = get_content_type(request)
    data = build_result_template(rows)
    if ct == 'json':
        # todo: move json to template
        return json(data)
    else:
        html_template = await get_template(ct)
        template = compiler.compile(html_template)
        output = template(data)
        if ct == 'html':
            return html(output)
        else:
            return text(output)

async def get_template(content_type):
    # todo: make templates a publisher
    if content_type == 'csv':
        return '{{#each headers}}{{header}},{{/each}}\r' \
                '{{#each rows}}{{#each row.cells}}{{cell}},{{/each}}\r{{/each}}'
    if content_type == 'html':
        return '<table>' \
                    '<style>table \n th {background-color: #dddddd;border: 1px solid black;} \n td {border: 1px solid black;} \n tr:nth-child(even) {background-color: #f2f2f2;} </style> ' \
                    '<tr>' \
                    '{{#each headers}} <th>{{header}}</th>{{/each}}' \
                    '</tr>' \
                    '{{#each rows}} <tr> {{#each row.cells}}<td>{{cell}}</td>{{/each}}</tr>{{/each}}' \
                    '</table>'
    raise Exception(f'there is no template for {content_type}')
