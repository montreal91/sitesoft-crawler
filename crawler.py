
import asyncio
import json
import re
import redis
import sys

from enum import Enum
from time import time
from urllib import request
from urllib.parse import urlparse
from urllib.error import HTTPError

from html_parser import MyHTMLParser

title_regexp = re.compile( r"<title>.*?</title>" )
m_redis = redis.Redis( host="127.0.0.1", port="6379" )

class ResultsField( Enum ):
    TITLE = "title"
    LINKS = "links"
    URL = "url"


def get_title( site ):
    title = title_regexp.findall( site )
    if title:
        title = title[0]
        title = title.replace( "<title>", "" )
        title = title.replace( "</title>", "" )
        return title
    else:
        return None

def get_full_url( host, url ):
    if url == "" or url == host:
        return host
    parsed_host = urlparse( host )
    parsed_url = urlparse( url )
    if parsed_url.scheme and parsed_url.netloc:
        return url
    elif parsed_url.netloc:
        if url[0] == "/":
            return "http:" + url
        else:
            return "http://" + url
    elif url[0] == "/":
        return parsed_host.scheme + "://" + parsed_host.netloc + url
    else:
        return host + url

@asyncio.coroutine
def load_page( url ):
    parse_res = urlparse( url )
    assert parse_res.scheme in ( "http", "https" ), "Url must contain 'http://' or 'https://'"
    res = request.urlopen( url )
    return str( res.readall() )

@asyncio.coroutine
def parse_page( url ):
    try:
        page = yield from load_page( url )
        title = get_title( page )

        parser = MyHTMLParser()
        parser.feed( page )
        res = {}
        res[ResultsField.URL.value] = url
        res[ResultsField.TITLE.value] = title
        res[ResultsField.LINKS.value] = [get_full_url( url, link ) for link in parser.links]
        return res
    except HTTPError:
        return None

def save_result_to_database( result, redis_connection ):
    link_target = redis_connection.get( 
        result[ResultsField.URL.value]
    )
    if link_target is None:
        result_json = json.dumps( {
            ResultsField.TITLE.value: result[ResultsField.TITLE.value],
            ResultsField.LINKS.value: result[ResultsField.LINKS.value]
        } )
        redis_connection.set( 
            result[ResultsField.URL.value],
            result_json
        )

def crawler( seed_url, depth ):
    visited_pages = set()
    urls_to_parse = [seed_url]

    for i in range( depth ):
        tasks = [parse_page( url ) for url in urls_to_parse if url not in visited_pages]
        parse_loop = asyncio.get_event_loop()
        results = parse_loop.run_until_complete( 
            asyncio.gather( *tasks )
        )
        visited_pages = visited_pages.union( set( urls_to_parse ) )

        for res in results:
            if res is not None:
                save_result_to_database( res, m_redis )

        urls_to_parse = [url for res in results if res is not None for url in res[ResultsField.LINKS.value] ]

def get_page_title( url, redis_connection ):
    db_res = redis_connection.get( url )
    if db_res is not None:
        res = json.loads( db_res.decode( "utf-8" ) )
        return res[ResultsField.TITLE.value]
    else:
        return None

def get_links_from_page( url, redis_connection ):
    db_res = redis_connection.get( url )
    if db_res is not None:
        js = json.loads( db_res.decode( "utf-8" ) )
        return [( link, get_page_title( link, redis_connection ) ) for link in js[ResultsField.LINKS.value]]

def print_help():
    print( "Usage: python crawler.py command url [-options]" )
    print( "Where commands include:" )
    print( "\tload: starts crawler which scrapes pages and saves links to the database" )
    print( "\t get: looks for specified url in the database and gets links from this page if they exist." )
    print( "\thelp: prints this help." )
    print( "Options:" )
    print( "\t-d --depth <value> specifies depth of the search. Use with 'load' option." )
    print( "\t-n <value> specifies number of rows in the output of 'get' command." )


def process_load( args ):
    url = args[0]
    if args[1] == "--depth" or args[1] == "-d":
        start_time = time()
        crawler( url, int( args[2] ) )
        end_time = time()
        print( "Execution time:", round( end_time - start_time, 2 ), "seconds" )
    else:
        print( "Please, specify the search depth." )
        print( "Use '-d <depth>' or --depth <depth>." )


def process_get( args ):
    url = args[0]
    if args[1] == "-n":
        row_num = int( args[2] )
        res = get_links_from_page( url, m_redis )
        if res:
            i = 0
            while i < row_num and len( res ) > 0:
                row = res.pop()
                print( *row )
                i += 1
        else:
            print( "There are no results for this page." )


if __name__ == '__main__':
    try:
        if sys.argv[1] == "load":
            process_load( sys.argv[2:] )
        if sys.argv[1] == "get":
            process_get( sys.argv[2:] )
    except IndexError:
        print_help()
    except:
        print( "Something went wrong." )
