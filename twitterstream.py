import oauth2 as oauth
import urllib2 as urllib

# See Assginment 1 instructions or README for how to get these credentials
consumer_key = "QEaAIck1jISZ1llMnXIvpQFAB"
consumer_secret = "OS4JwiG2GHCzDTpt7KZB1rNavsFZKNiIMLoyygQfM15CoChUxI"
access_token_key = "4854948347-xVEWwYjdsoyyzAMfepBEO03L5x4pC7cHvOvq9XY"
access_token_secret = "J5X6Y17EQG88maJ3s2N6MPxAfqVVMtysfqUBw7o0u153g"

_debug = 0

oauth_token    = oauth.Token(key=access_token_key, secret=access_token_secret)
oauth_consumer = oauth.Consumer(key=consumer_key, secret=consumer_secret)

signature_method_hmac_sha1 = oauth.SignatureMethod_HMAC_SHA1()

http_method = "GET"


http_handler  = urllib.HTTPHandler(debuglevel=_debug)
https_handler = urllib.HTTPSHandler(debuglevel=_debug)

'''
Construct, sign, and open a twitter request
using the hard-coded credentials above.
'''
def twitterreq(url, method, parameters):
  req = oauth.Request.from_consumer_and_token(oauth_consumer,
                                             token=oauth_token,
                                             http_method=http_method,
                                             http_url=url,
                                             parameters=parameters)

  req.sign_request(signature_method_hmac_sha1, oauth_consumer, oauth_token)

  headers = req.to_header()

  if http_method == "POST":
    encoded_post_data = req.to_postdata()
  else:
    encoded_post_data = None
    url = req.to_url()

  opener = urllib.OpenerDirector()
  opener.add_handler(http_handler)
  opener.add_handler(https_handler)

  response = opener.open(url, encoded_post_data)

  return response

def fetchsamples():
  url = "https://stream.twitter.com/1.1/statuses/sample.json?language=en"
  #can change the url to the newer REST API v. 1.1 instead
  parameters = []
  response = twitterreq(url, "GET", parameters)
  for line in response:
    print line.strip()

if __name__ == '__main__':
  fetchsamples()