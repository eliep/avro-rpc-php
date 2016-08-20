# Avro RPC client for PHP

See [Avro](http://avro.apache.org/) for a full documentation on Avro and its 
usage in PHP.

This library is a fork of the original Avro library, only adding a php Avro RPC client


## Installation
Add the following to your composer.json require section 

```
"eliep/avro-rpc-php": "^1.7.7.0"
```

and run
 
```bash
composer install
```

## Usage

### Create a client for your protocol 

```php

/**
 * $protocol: your Avro protocol as a string
 * $serverHost: Avro RPC Server Host
 * $serverPort: Avro RPC Server Port
 **/
 
// Parse your avro protocol
$avroProtocol = AvroProtocol::parse($protocol);
// Connect to the server
$client = NettyFramedSocketTransceiver::create($serverHost, $serverPort);
// Retrieve a client
$requestor = new Requestor($avroProtocol, $client);
```
### Request the server
Simply use the `request` method of the `Requestor` instance.
This method has two parameters:

  - the message name as defined in your avro protocol
  - an array of named parameter as defined by the request part of your message

for example, if your protocol is:

```json
{
 ...
 "types": [
     {"type": "record", "name": "SimpleRequest",
      "fields": [{"name": "subject",   "type": "string"}]
     },
     {"type": "record", "name": "SimpleResponse",
      "fields": [{"name": "response",   "type": "string"}]
     }
 ],

 "messages": {
     "testSimpleRequestResponse": {
         "doc" : "Simple Request Response",
         "request": [{"name": "message", "type": "SimpleRequest"}],
         "response": "SimpleResponse"
     }
 }
}
```

```php
try {
  $response = $requestor->request('testSimpleRequestResponse', array("message" => array("subject" => "pong")));
  echo "Response received: ".json_encode($response)."\n";
} catch (AvroRemoteException $e) {
  // an error occured on the server while handling the request.
}
```

## Example
An RPC client example is located in the `examples/sample_rpc_client.php`. It can be used 
with the `examples/sample_rpc_server.php` to test the client/server communication.
 
 - Run `php examples/sample_rpc_server.php`
 - Then run `php examples/sample_rpc_client.php` in another console.

## Test
Test can be run with:
```shell
phpunit test/AllTests.php
```

These are mostly the original Avro test except for the `test/IpcTest.php` files