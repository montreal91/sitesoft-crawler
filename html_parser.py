
from html.parser import HTMLParser
from html.entities import name2codepoint

class MyHTMLParser( HTMLParser ):
    def __init__( self ):
        super( MyHTMLParser, self ).__init__()
        self._title = None
        self._links = set()

    @property
    def title( self ):
        return self._title

    @property
    def links( self ):
#         if self._links:
        return self._links
#         else:
#             return None

    def handle_starttag( self, tag, attrs ):
        if tag == "a":
            self.handle_a_tag_attributes( attrs )

    # Internal
    def handle_a_tag_attributes( self, attrs ):
        for attr in attrs:
            if attr[0] == "href" and attr[1]:
                self._links.add( attr[1] )

