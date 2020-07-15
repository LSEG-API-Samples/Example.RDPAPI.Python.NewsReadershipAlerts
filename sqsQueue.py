#=============================================================================
# Module for polling and extracting news or research alerts from an AWS SQS Queue
# This module uses boto3 library from Anazon for fetching messages
# It uses pycryptodome - for AES GCM decryption
#=============================================================================

import boto3
import json
import base64
from Crypto.Cipher import AES
import html

REGION = 'us-east-1'

#=============================================================================
def decrypt(key, source):
	GCM_AAD_LENGTH = 16
	GCM_TAG_LENGTH = 16
	GCM_NONCE_LENGTH = 12
	key = base64.b64decode(key)
	cipherText = base64.b64decode(source)
	
	aad = cipherText[:GCM_AAD_LENGTH]
	nonce = aad[-GCM_NONCE_LENGTH:] 
	tag = cipherText[-GCM_TAG_LENGTH:]
	encMessage = cipherText[GCM_AAD_LENGTH:-GCM_TAG_LENGTH]
	
	cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
	cipher.update(aad)
	decMessage = cipher.decrypt_and_verify(encMessage, tag)
	return decMessage



#=============================================================================
def processPayload(payloadText, callback, storefile):
	pl = json.loads(payloadText)
	# handover the decoded message to calling module
	if callback is not None:
		callback(pl)
	elif storefile is not None:
		storefile.write(json.dumps(pl)+",\n")  
	else:
		#print(json.dumps(pl, indent=2))
		print(json.dumps(pl))



#=============================================================================
def startPolling(accessID, secretKey, sessionToken, endpoint, cryptographyKey, callback=None, storefile=None):
    # open file to store
        
	# create a SQS session
	session = boto3.Session(
		aws_access_key_id = accessID,
		aws_secret_access_key = secretKey,
		aws_session_token = sessionToken,
		region_name = REGION
	)

	sqs = session.client('sqs')

	print('Polling messages from queue...')
	while 1: 
		resp = sqs.receive_message(QueueUrl = endpoint, WaitTimeSeconds = 10)
		
		if 'Messages' in resp:
			messages = resp['Messages']
			# print and remove all the nested messages
			for message in messages:
				mBody = message['Body']
				# decrypt this message
				m = decrypt(cryptographyKey, mBody)
				processPayload(m, callback, storefile)
				# *** accumulate and remove all the nested message
				sqs.delete_message(QueueUrl = endpoint, ReceiptHandle = message['ReceiptHandle'])



#=============================================================================
if __name__ == "__main__":
	print("SQS module cannot run standalone. Please use newsAlerts or researchAlerts")
