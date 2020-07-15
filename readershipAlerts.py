#=============================================================================
# Refinitiv Data Platform demo app to subscribe to READERSHIP alerts
#=============================================================================
import requests
import json
import rdpToken
import sqsQueue
import atexit
import sys
import boto3
from botocore.exceptions import ClientError
import datetime

# Application Constants
base_URL = "https://api.refinitiv.com"
#base_URL = "eds-gw-204891-main-pre.tr-fr-nonprod.aws-int.thomsonreuters.com"
#base_URL = "https://api.ppe.refinitiv.com"

RDP_version = "/beta1"

currentSubscriptionID = None

#=============================================================================
def subscribeToNews():
	category_URL = "/message-services"
	endpoint_URL = "/news-readership/story/subscriptions"
	RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL
	requestData = {
		"transport": {
			"transportType": "AWS-SQS"
		}
	};
	
	# get the latest access token
	accessToken = rdpToken.getToken()
	hdrs = {
		"Authorization": "Bearer " + accessToken,
		"Content-Type": "application/json"
	};
	
	dResp = requests.post(RESOURCE_ENDPOINT, headers = hdrs, data = json.dumps(requestData));
	if dResp.status_code != 200:
		raise ValueError("Unable to subscribe. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		jResp = json.loads(dResp.text)
		return jResp["transportInfo"]["endpoint"], jResp["transportInfo"]["cryptographyKey"], jResp["subscriptionID"]



#=============================================================================
def getCloudCredentials(endpoint):
	category_URL = "/auth/cloud-credentials"
	endpoint_URL = "/"
	RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL
	requestData = {
		"endpoint": endpoint
	};
	
	# get the latest access token
	accessToken = rdpToken.getToken()
	dResp = requests.get(RESOURCE_ENDPOINT, headers = {"Authorization": "Bearer " + accessToken}, params = requestData);
	if dResp.status_code != 200:
		raise ValueError("Unable to get credentials. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		jResp = json.loads(dResp.text)
		return jResp["credentials"]["accessKeyId"], jResp["credentials"]["secretKey"], jResp["credentials"]["sessionToken"]




#=============================================================================
def startNewsAlerts():
	global currentSubscriptionID
	global filestore
	if filestore is not None:
		print("Opening file "+filestore)
		storefile = open(filestore, 'w+', encoding='utf-8')
	else:
		storefile = None

	try:
		print("Subscribing to News Readership Alerts...");
		endpoint, cryptographyKey, currentSubscriptionID = subscribeToNews()
		print("  Queue endpoint: %s" % (endpoint) )
		print("  Subscription ID: %s" % (currentSubscriptionID) )
		
		# unsubscribe before shutting down
		atexit.register(removeSubscription)

		while 1:
			try:
				print("Getting credentials to connect to AWS Queue...")
				accessID, secretKey, sessionToken = getCloudCredentials(endpoint)
				print("Queue access ID: %s" % (accessID) )
				print(datetime.datetime.utcnow().isoformat()[:-7]+" Getting Readership Alerts, press BREAK to exit and delete subscription...")
				sqsQueue.startPolling(accessID, secretKey, sessionToken, endpoint, cryptographyKey, None, storefile)
			except ClientError as e:
				print("Cloud credentials exprired! "+str(e))
	except KeyboardInterrupt:
		print("User requested break, cleaning up...")
		storefile.close()
		sys.exit(0)	



#=============================================================================
def removeSubscription():
	category_URL = "/message-services"
	endpoint_URL = "/news-readership/story/subscriptions"
	RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL
	# get the latest access token
	accessToken = rdpToken.getToken()

	if currentSubscriptionID:
		print("Deleting the open alerts subscription")
		dResp = requests.delete(RESOURCE_ENDPOINT, headers = {"Authorization": "Bearer " + accessToken}, params = {"subscriptionID": currentSubscriptionID});
	else:
		print("Deleting ALL open alerts subscription")
		dResp = requests.delete(RESOURCE_ENDPOINT, headers = {"Authorization": "Bearer " + accessToken});

	if dResp.status_code > 299:
		print(dResp)
		print("Warning: unable to remove subscription. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		print("Alerts unsubscribed!")
			


#=============================================================================
def showActiveSubscriptions():
	category_URL = "/message-services"
	endpoint_URL = "/news-readership/story/subscriptions"
	RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL
	# get the latest access token
	accessToken = rdpToken.getToken()

	print("Getting all open news subscriptions")
	dResp = requests.get(RESOURCE_ENDPOINT, headers = {"Authorization": "Bearer " + accessToken});

	if dResp.status_code != 200:
		raise ValueError("Unable to get subscriptions. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		jResp = json.loads(dResp.text)
		print(json.dumps(jResp, indent=2))

			
			
#=============================================================================
if __name__ == "__main__":
	filestore = None
	if len(sys.argv) == 3:
		filestore = sys.argv[2]
	if len(sys.argv) > 1:
		if sys.argv[1] == '-l':
			showActiveSubscriptions()
		elif sys.argv[1] == '-d':
			removeSubscription()
		elif sys.argv[1] == '-s':
			startNewsAlerts()
	else:
		print("Arguments:");
		print("  -l List active subscriptions");
		print("  -d Delete all subscriptions");
		print("  -s Subscribe to alerts");
		print("  [optional - filename to store the stream]");

