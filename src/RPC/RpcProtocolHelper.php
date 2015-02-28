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
  private $md5;
  
  public function __construct($json, $md5 = null) {
    $this->json = $json;
    $this->protocol = \AvroProtocol::parse($json);
    $this->md5 = ($md5 != null) ? $md5 : md5($this->__toString(), true);
  }
  
  public function getProtocol() { return $this->protocol; }
  public function getMD5() { return $this->md5; }
  public function getRequestHandshakeSchema() { return \AvroSchema::parse($this->jsonRequestHandshake); }
  public function getResponseHandshakeSchema() { return \AvroSchema::parse($this->jsonResponseHandshake); }
  public function getMetadataSchema() { return \AvroSchema::parse($this->jsonMetadata); }
  public function getRequestSchemas($method) {
    return $this->getProtocol()->messages[$method]->request;
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
    $writer = new \AvroIODatumWriter($this->getRequestSchemas($method));
    $writer->write($datum, $encoder);
      /*
    $schemas = $this->getRequestSchemas($method);
    for ($i = 0; $i < count($schemas); $i++) {
      $writer = new \AvroIODatumWriter($schemas[$i]);
      $writer->write($datum[$i], $encoder);
    }
    */
  }
  
  public function readRequest($decoder, $method) {
    $schemas = $this->getRequestSchemas($method);
    $reader = new \AvroIODatumReader($schemas);
    /*
      $params[] = $reader->read($decoder);
    $params = array();
    for ($i = 0; $i < count($schemas); $i++) {
      $reader = new \AvroIODatumReader($schemas[$i]);
      $params[] = $reader->read($decoder);
    }
    return $params;
    */
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
  
  
  /**
   * @returns string the JSON-encoded representation of this Avro schema.
   */
  public function __toString() {
    return json_encode($this->to_avro());
  }
  
  /**
   * @returns mixed
   */
  public function to_avro()
  {
    $avro = array("protocol" => $this->getProtocol()->name, "namespace" => $this->getProtocol()->namespace);//, "types" => , "messages" => );
    
    $types = array();
    $messages = array();
    foreach ($this->getProtocol()->messages as $name => $msg) {
      
      foreach ($msg->request->fields() as $field) {
        $field_fullname =$field->type->fullname();
        if ($this->getProtocol()->schemata->has_name($field_fullname)) {
          $types[] = $this->getProtocol()->schemata->schema($field_fullname)->to_avro();
        }
      }
      
      $messages[$name] = array(
        "request" => $msg->request->to_avro()
      );
      
      if (!is_null($msg->response)) {
        $response_type = $msg->response->type();
        if (\AvroSchema::is_named_type($response_type)) {
          $response_type = $msg->response->qualified_name();
          $types[] = $this->getProtocol()->schemata->schema($msg->response->fullname())->to_avro();
        }

        $messages[$name]["response"] = $response_type;
      }
    }
    
    $avro["types"] = $types;
    $avro["messages"] = $messages;
    
    return $avro;
  }


}