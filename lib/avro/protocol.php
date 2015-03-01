<?php
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @package Avro
 */

/**
 * Avro library for protocols
 * @package Avro
 */
class AvroProtocol
{
  public $name;
  public $namespace;
  public $doc = null;
  public $schemata;
  public $messages;

  public static function parse($json)
  {
    if (is_null($json))
      throw new AvroProtocolParseException( "Protocol can't be null");

    $protocol = new AvroProtocol();
    $protocol->real_parse(json_decode($json, true));
    return $protocol;
  }

  function real_parse($avro)
  {
    $this->protocol = $avro["protocol"];
    $this->namespace = $avro["namespace"];
    $this->schemata = new AvroNamedSchemata();
    $this->name = $avro["protocol"];
    
    if (isset($avro["doc"]))
      $this->doc = $avro["doc"];

    if (!is_null($avro["types"])) {
        $types = AvroSchema::real_parse($avro["types"], $this->namespace, $this->schemata);
    }

    if (!is_null($avro["messages"])) {
      foreach ($avro["messages"] as $messageName => $messageAvro) {
        $message = new AvroProtocolMessage($messageName, $messageAvro, $this);
        $this->messages{$messageName} = $message;
      }
    }
  }
  
  public function request_schemas($method)
  {
    $schemas = array();
    $msgs = $this->getProtocol()->messages[$method];
    foreach ($msgs->request->fields() as $field) {
      $schemas[] = $field->type();
    }
    return $schemas;
  }
  
  /**
   * @return string a md5 hash of this Avro Protocol
   */
  public function md5()
  {
    return md5($this->__toString(), true);
  }
  
  /**
   * @returns string the JSON-encoded representation of this Avro schema.
   */
  public function __toString()
  {
    return json_encode($this->to_avro());
  }
  
  /**
   * Internal represention of this Avro Protocol.
   * @returns mixed
   */
  public function to_avro()
  {
    $avro = array("protocol" => $this->name, "namespace" => $this->namespace);//, "types" => , "messages" => );
    
    if (!is_null($this->doc))
      $avro["doc"] = $this->doc;
    
    $types = array();
    $messages = array();
    foreach ($this->messages as $name => $msg) {
      
      $messages[$name] = array();
      
      if (!is_null($msg->doc))
        $messages[$name]["doc"] = $msg->doc;
      
      foreach ($msg->request->fields() as $field) {
        $field_fullname =$field->type->fullname();
        if ($this->schemata->has_name($field_fullname)) {
          $types[] = $this->schemata->schema($field_fullname)->to_avro();
        }
      }
      
      $messages[$name] = array(
        "request" => $msg->request->to_avro()
      );
      
      if ($msg->is_one_way()) {
        $messages[$name]["response"] = "null";
        $messages[$name]["one-way"] = true;
      } else {
        
        if (!is_null($msg->response)) {
          $response_type = $msg->response->type();
          if (AvroSchema::is_named_type($response_type)) {
            $response_type = $msg->response->qualified_name();
            $types[] = $this->schemata->schema($msg->response->fullname())->to_avro();
          }
  
          $messages[$name]["response"] = $response_type;
        }
        
        if (!is_null($msg->errors)) {
          $messages[$name]["errors"] = array();
          
          foreach ($msg->errors->schemas() as $error) {
            $error_type = $error->type();
            if (AvroSchema::is_named_type($error_type)) {
              $error_type = $error->qualified_name();
              $types[] = $this->schemata->schema($error->fullname())->to_avro();
            }
    
            $messages[$name]["errors"][] = $error_type;
          }
        }
      }
    }
    
    $avro["types"] = $types;
    $avro["messages"] = $messages;
    
    return $avro;
  }
}

class AvroProtocolMessage
{

  public $doc = null;
  public $name;
  /**
   * @var AvroRecordSchema $request
   */
  public $request;
  public $response = null;
  public $errors = null;
  
  private $is_one_way = false;

  public function __construct($name, $avro, $protocol)
  {
    $this->name = $name;
    
    if (array_key_exists('doc', $avro))
      $this->doc = $avro["doc"];
      
    $this->request = new AvroRecordSchema(new AvroName($name, null, $protocol->namespace), null, $avro{'request'}, $protocol->schemata, AvroSchema::REQUEST_SCHEMA);

    if (array_key_exists('response', $avro))
      $this->response = $protocol->schemata->schema_by_name(new AvroName($avro{'response'}, $protocol->namespace, $protocol->namespace));
    else $avro["response"] = "null";
      
    if ($this->response == null)
        $this->response = new AvroPrimitiveSchema($avro{'response'});
    
    if (array_key_exists('errors', $avro)) {
      if (!is_array($avro["errors"]))
        throw new AvroProtocolParseException( "Errors must be an array");
      
      $errors = array();
      foreach ($avro["errors"] as $error_type) {
        $error_schema = $protocol->schemata->schema_by_name(new AvroName($error_type, $protocol->namespace, $protocol->namespace));
        if (is_null($error_schema))
          throw new AvroProtocolParseException( "Error type $error_type is unknown");
        
        $errors[] = $error_schema->qualified_name();
      }
      $this->errors = new AvroUnionSchema($errors, $protocol->namespace, $protocol->schemata);
    }
    
    if (isset($avro["one-way"]))
      $this->is_one_way = $avro["one-way"];
    
    if ($this->is_one_way && $this->response->type() != AvroSchema::NULL_TYPE)
      throw new AvroProtocolParseException( "One way message $name can't have a reponse");
    
    if ($this->is_one_way && !is_null($this->errors))
      throw new AvroProtocolParseException( "One way message $name can't have errors");
    
  }
  
  public function is_one_way()
  {
    return $this->is_one_way;
  }
}

class AvroProtocolParseException extends AvroException {};
