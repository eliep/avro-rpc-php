<?php


namespace Avro\RPC;

class RpcProtocolHelper {
  
  private $jsonRequestHandshake = <<<HSR
{
  "type": "record",
  "name": "HandshakeRequest", "namespace":"org.apache.avro.ipc",
  "fields": [
    {"name": "clientHash",
     "type": {"type": "fixed", "name": "MD5", "size": 16}},
    {"name": "clientProtocol", "type": ["null", "string"]},
    {"name": "serverHash", "type": "MD5"},
    {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]}
  ]
}
HSR;

  private $jsonResponseHandshake = <<<HSR
{
  "type": "record",
  "name": "HandshakeResponse", "namespace": "org.apache.avro.ipc",
  "fields": [
    {"name": "match",
     "type": {"type": "enum", "name": "HandshakeMatch",
              "symbols": ["BOTH", "CLIENT", "NONE"]}},
    {"name": "serverProtocol",
     "type": ["null", "string"]},
    {"name": "serverHash",
     "type": ["null", {"type": "fixed", "name": "MD5", "size": 16}]},
    {"name": "meta",
     "type": ["null", {"type": "map", "values": "bytes"}]}
  ]
}
HSR;

  private $jsonMetadata =  <<<META
{"type": "map", "values": "bytes"}
META;

  private $protocol;
  private $json;
  
  public function __construct($json) {
    $this->json = $json;
    $this->protocol = \AvroProtocol::parse($json);
  }
  
  public function getProtocol() { return $this->protocol; }
  public function getMD5() { return md5(json_encode(json_decode($this->json)), true); }
  public function getRequestHandshakeSchema() { return \AvroSchema::parse($this->jsonRequestHandshake); }
  public function getResponseHandshakeSchema() { return \AvroSchema::parse($this->jsonResponseHandshake); }
  public function getMetadataSchema() { return \AvroSchema::parse($this->jsonMetadata); }
  public function getRequestSchemas($method) {
    $schemas = array();
    $msgs = $this->getProtocol()->messages[$method];
    foreach ($msgs->request->fields() as $field) {
      $schemas[] = $field->type();
    }
    return $schemas;
  }
  
  public function getResponseSchema($method) {
    return $this->getProtocol()->messages[$method]->response;
  }
  
  public function writeRequestHandshake($encoder, $with_client_protocol = false) {
    $datum = array("clientHash" => $this->getMD5(), "serverHash" => $this->getMD5(), "meta" => null);
    $datum["clientProtocol"] = ($with_client_protocol) ? $this->json : null;
    $writer = new \AvroIODatumWriter($this->getRequestHandshakeSchema());
    $writer->write($datum, $encoder);
  }
    
  public function readRequestHandshake($decoder) {
    $reader = new \AvroIODatumReader($this->getRequestHandshakeSchema());
    return $reader->read($decoder);
  }
  
  public function writeResponseHandshake($encoder) {
    $datum = array("match" => "BOTH", "serverHash" => $this->getMD5(), "serverProtocol" => null, "meta" => null);
    $writer = new \AvroIODatumWriter($this->getResponseHandshakeSchema());
    $writer->write($datum, $encoder);
  }
    
  public function readResponseHandshake($decoder) {
    $reader = new \AvroIODatumReader($this->getResponseHandshakeSchema());
    return $reader->read($decoder);
  }
  
  public function writeMetadata($encoder, $datum = array() ) {
    $writer = new \AvroIODatumWriter($this->getMetadataSchema());
    $writer->write($datum, $encoder);
  }
  
  public function readMetadata($decoder) {
    $reader = new \AvroIODatumReader($this->getMetadataSchema());
    return $reader->read($decoder);
  }
  
  public function writeRequest($encoder, $method, $datum = array()) {
    $schemas = $this->getRequestSchemas($method);
    for ($i = 0; $i < count($schemas); $i++) {
      $writer = new \AvroIODatumWriter($schemas[$i]);
      $writer->write($datum[$i], $encoder);
    }
  }
  
  public function readRequest($decoder, $method) {
    $schemas = $this->getRequestSchemas($method);
    $params = array();
    for ($i = 0; $i < count($schemas); $i++) {
      $reader = new \AvroIODatumReader($schemas[$i]);
      $params[] = $reader->read($decoder);
    }
    return $params;
  }
  
  public function writeResponse($encoder, $method, $datum = array()) {
    $writer = new \AvroIODatumWriter($this->getResponseSchema($method));
    $writer->write($datum, $encoder);
  }
  
  public function readResponse($decoder, $method) {
    $reader = new \AvroIODatumReader($this->getResponseSchema($method));
    return $reader->read($decoder);
  }
  
  public function readError($decoder) {
    $reader = new \AvroIODatumReader(\AvroSchema::parse('["string"]'));
    return $reader->read($decoder);
  }
  
}