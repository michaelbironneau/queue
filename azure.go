package queue

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

//AzureQueue is a queue backed by an Azure Service Bus Queue. Its methods are safe for use by multiple goroutines.
type AzureQueue struct {
	namespace string
	saKey     string
	saValue   []byte
	url       string
	client    *http.Client
}

const azureQueueURL = "https://%s.servicebus.windows.net:443/%s/"

//NewAzureQueue creates a new queue from the given parameters. Their meaning can be found in the MSDN docs at:
//  https://msdn.microsoft.com/en-us/library/azure/dn798895.aspx
func NewAzureQueue(namespace string, sharedAccessKeyName string, sharedAccessKeyValue string, queuePath string) *AzureQueue {
	/*ss, err := base64.StdEncoding.DecodeString(string(sharedAccessKeyValue))
	if err != nil {
		panic(err)
	}*/
	return &AzureQueue{
		namespace: namespace,
		saKey:     sharedAccessKeyName,
		saValue:   []byte(sharedAccessKeyValue),
		url:       fmt.Sprintf(azureQueueURL, namespace, queuePath),
		client:    &http.Client{},
	}
}

func (aq *AzureQueue) request(url string, method string) (*http.Request, error) {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", aq.authHeader(url, aq.signatureExpiry(time.Now())))
	return req, nil
}

func (aq *AzureQueue) requestWithBody(url string, method string, body []byte) (*http.Request, error) {
	req, err := http.NewRequest(method, url, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", aq.authHeader(url, aq.signatureExpiry(time.Now())))
	return req, nil
}

//Succeed confirms that the request has been processed and permanently removes it from the queue.
//
//For more information see https://msdn.microsoft.com/en-us/library/azure/hh780768.aspx.
func (aq *AzureQueue) Succeed(item *Item) error {
	req, err := aq.request(aq.url+"messages/"+item.ID+"/"+item.LockToken, "DELETE")

	if err != nil {
		return err
	}

	resp, err := aq.client.Do(req)

	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	defer resp.Body.Close()

	b, _ := ioutil.ReadAll(resp.Body)

	return fmt.Errorf("Got error code %v with body %s", resp.StatusCode, string(b))
}

//Send enqueues a new item.
//
//For more information see https://msdn.microsoft.com/en-us/library/azure/hh780737.aspx.
func (aq *AzureQueue) Send(item *Item) error {
	req, err := aq.requestWithBody(aq.url+"messages/", "POST", item.Request)

	if err != nil {
		return err
	}

	resp, err := aq.client.Do(req)

	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		return nil
	}

	defer resp.Body.Close()

	b, _ := ioutil.ReadAll(resp.Body)

	return fmt.Errorf("Got error code %v with body %s", resp.StatusCode, string(b))
}

//Fail unlocks the request for processing by other workers. It should be used if the worker could not complete the request. In particular,
//because the lock token eventually expires, workers should not implement much (if any) retry logic as the token might expire before the
//retried code succeeds, leading to a request being processed multiple times.
//
//For more information see https://msdn.microsoft.com/en-us/library/azure/hh780737.aspx.
func (aq *AzureQueue) Fail(item *Item) error {
	req, err := aq.request(aq.url+"messages/"+item.ID+"/"+item.LockToken, "PUT")

	if err != nil {
		return err
	}

	resp, err := aq.client.Do(req)

	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	defer resp.Body.Close()

	b, _ := ioutil.ReadAll(resp.Body)

	return fmt.Errorf("Got error code %v with body %s", resp.StatusCode, string(b))
}

//Next retrieves the next item from the queue. If there is none the first return parameter will be nil.
//
//For more information see https://msdn.microsoft.com/en-us/library/azure/hh780722.aspx.
func (aq *AzureQueue) Next() (*Item, error) {
	req, err := aq.request(aq.url+"messages/head?timeout=60", "POST")

	if err != nil {
		return nil, err
	}
	resp, err := aq.client.Do(req)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	brokerProperties := resp.Header.Get("BrokerProperties")

    fmt.Println(brokerProperties) //ejh debug

	var props map[string]interface{}

	if err := json.Unmarshal([]byte(brokerProperties), &props); err != nil {
		return nil, fmt.Errorf("Error unmarshalling BrokerProperties: %v", err)
	}
	var (
		messageID string
		lockToken string
		ok        bool
	)
	if messageID, ok = props["MessageId"].(string); !ok {
		return nil, fmt.Errorf("BrokerProperties did not include MessageId or it was not a string")
	}

	if lockToken, ok = props["LockToken"].(string); !ok {
		return nil, fmt.Errorf("BrokerProperties did not include LockToken or it was not a string")
	}

	message, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return nil, fmt.Errorf("Error reading message body")
	}

	return &Item{ID: messageID, LockToken: lockToken, Request: message}, nil
}

//signatureExpiry returns the expiry for the shared access signature for the next request.
//
//It's translated from the Python client:
// https://github.com/Azure/azure-sdk-for-python/blob/master/azure-servicebus/azure/servicebus/servicebusservice.py
func (aq *AzureQueue) signatureExpiry(from time.Time) string {
	t := from.Add(300 * time.Second).Round(time.Second).Unix()
	return strconv.Itoa(int(t))
}

//signatureURI returns the canonical URI according to Azure specs.
//
//It's translated from the Python client:
//https://github.com/Azure/azure-sdk-for-python/blob/master/azure-servicebus/azure/servicebus/servicebusservice.py
func (aq *AzureQueue) signatureURI(uri string) string {
	return strings.ToLower(url.QueryEscape(uri)) //Python's urllib.quote and Go's url.QueryEscape behave differently. This might work, or it might not...like everything else to do with authentication in Azure.
}

//stringToSign returns the string to sign.
//
//It's translated from the Python client:
//https://github.com/Azure/azure-sdk-for-python/blob/master/azure-servicebus/azure/servicebus/servicebusservice.py
func (aq *AzureQueue) stringToSign(uri string, expiry string) string {
	return uri + "\n" + expiry
}

//signString returns the HMAC signed string.
//
//It's translated from the Python client:
//https://github.com/Azure/azure-sdk-for-python/blob/master/azure-servicebus/azure/servicebus/_common_conversion.py
func (aq *AzureQueue) signString(s string) string {
	h := hmac.New(sha256.New, aq.saValue)
	h.Write([]byte(s))
	encodedSig := base64.StdEncoding.EncodeToString(h.Sum(nil))
	return url.QueryEscape(encodedSig)
}

//authHeader returns the value of the Authorization header for requests to Azure Service Bus.
//
//It's translated from the Python client:
//https://github.com/Azure/azure-sdk-for-python/blob/master/azure-servicebus/azure/servicebus/servicebusservice.py
func (aq *AzureQueue) authHeader(uri string, expiry string) string {
	u := aq.signatureURI(uri)
	s := aq.stringToSign(u, expiry)
	sig := aq.signString(s)
	return fmt.Sprintf("SharedAccessSignature sig=%s&se=%s&skn=%s&sr=%s", sig, expiry, aq.saKey, u)
}
