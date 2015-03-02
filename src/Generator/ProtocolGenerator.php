<?php

namespace Avro\Generator;

use Avro\RPC\RpcProtocolHelper;

class ProtocolGenerator {
  
    private $protocol_tpl =<<< PT
<?php

namespace PT_NAMESPACE;

class PT_CLASSNAME extends \Requestor {
  
  private \$json_protocol =
PT_JSON
  
  public function __construct(\$host, \$port) {
    \$client = \NettyFramedSocketTransceiver::create(\$host, \$port);
    parent::__construct(\AvroProtocol::parse(\$this->json_protocol), \$client);
  }
  
  public function getJsonProtocol() { return \$this->jsonProtocol; }
  
  public function close() { return \$this->transceiver->close(); }
  
PT_CLIENT_FUNCTIONS
  
}
PT;

    private $client_function_tpl = <<<CFT
    
  public function CFT_NAME(CFT_PARAMS) {
    return \$this->request('CFT_NAME', array(CFT_ASSOC_PARAMS));
  }
    
CFT;
  
  
  public function generates($input_folder, $output_folder, $namespace_prefix = null) {
    if (file_exists($input_folder)) {
      $path = realpath($input_folder);
      foreach (new \RecursiveIteratorIterator(new \RecursiveDirectoryIterator($path)) as $filename) {
        if (!is_dir($filename)) {
          $this->write($filename, $output_folder, $namespace_prefix);
        }
      }
    }
  }
  
  public function write($input_filename, $output_folder, $namespace_prefix) {
    $protocol_json = file_get_contents($input_filename);
    $protocol = \AvroProtocol::parse($protocol_json);
    
    $working_tpl = $this->protocol_tpl;
    $filename = $this->getFilename($protocol);
    $subdirectory = $this->getSubdirectory($protocol);
    $working_tpl = $this->generate($protocol, $protocol_json, $namespace_prefix, $working_tpl);
    
    if (!file_exists($output_folder.$subdirectory))
      mkdir($output_folder.$subdirectory, 0755, true);
    file_put_contents($output_folder.$subdirectory."/".$filename, $working_tpl);
  }
  
  public function generate($protocol, $protocol_json, $namespace_prefix, $working_tpl) {
    $namespace = $this->getNamespace($protocol, $namespace_prefix);
    $working_tpl = str_replace("PT_NAMESPACE", $namespace, $working_tpl);
    
    $classname = $this->getClassname($protocol);
    $working_tpl = str_replace("PT_CLASSNAME", $classname, $working_tpl);
    
    $json = $this->getJson($protocol_json);
    $working_tpl = str_replace("PT_JSON", $json, $working_tpl);
    
    $client_functions = $this->getClientFunctions($protocol);
    $working_tpl= str_replace("PT_CLIENT_FUNCTIONS", $client_functions, $working_tpl);
    
    return $working_tpl;
  }
  
  public function getNamespace($protocol, $namespace_prefix = null) {
    $namespace_token = explode(".", $protocol->namespace);
    array_walk($namespace_token, function(&$token) { $token = ucfirst($token); });
    return (!is_null($namespace_prefix)) ? $namespace_prefix."\\". implode("\\", $namespace_token) : implode("\\", $namespace_token);
  }
  
  public function getClassname($protocol) {
    return $protocol->name."Requestor";
  }
  
  public function getFilename($protocol) {
    return $this->getClassname($protocol).".php";
  }
  
  public function getSubdirectory($protocol) {
    return "/".str_replace("\\", "/", $this->getNamespace($protocol));
  }
  
  
  public function getJson($protocol_json) {
    return "<<<PROTO\n".json_encode(json_decode($protocol_json))."\nPROTO;\n";
  }
  
  public function getClientFunctions($protocol) {
    $client_functions = array();
    
    
    $messages = $protocol->messages;
    foreach ($messages as $msg_name => $msg_def) {
      $working_tpl = $this->client_function_tpl;
    
      $client_functions[$msg_name] = array();
      $client_functions_assoc[$msg_name] = array();
      foreach ($msg_def->request->fields() as $field) {
        //echo $field->name()."/".$field->type(). "\n";
        $client_functions[$msg_name][] = "$".$field->name();
        $client_functions_assoc[$msg_name][] = "'".$field->name()."'" . " => $" . $field->name();
      }
      $client_functions[$msg_name] = str_replace("CFT_PARAMS", implode(", ", $client_functions[$msg_name]), $working_tpl);
      $client_functions[$msg_name] = str_replace("CFT_ASSOC_PARAMS", implode(", ", $client_functions_assoc[$msg_name]), $client_functions[$msg_name]);
      $client_functions[$msg_name] = str_replace("CFT_NAME", $msg_name, $client_functions[$msg_name]);
      //echo $msg_def->response->fullname()."/".$msg_def->response->qualified_name()."/".$msg_def->response->type()."\n";
    }
    return implode("\n", $client_functions);
  }
}
/*

<?php

namespace Avro\Examples\Protocol\Fr\V3d\Avro;

use Avro\RPC\RpcProtocol;

class AsvProtocol extends RpcProtocol {
  
  private $jsonProtocol =
  <<<PROTO
{"protocol": "ASV",
 "namespace": "fr.v3d.avro",

 "types": [
     {"type": "record", "name": "Asv",
      "fields": [
          {"name": "a",   "type": "int"},
          {"name": "s", "type": "string"},
          {"name": "v", "type": "string"}
      ]
     },
     {"type": "record", "name": "Attention",
      "fields": [
          {"name": "status",   "type": "string"}
      ]
     }
     
 ],

 "messages": {
     "send": {
         "request": [{"name": "message", "type": "Asv"}],
         "response": "Attention"
     }
 }
}
PROTO;

  public function getJsonProtocol() {
    return $this->jsonProtocol;
  }
  
  public function send($message) {
    return $this->genericRequest(array($message));
  }
  
  public function sendImpl($callback) {
    $this->genericResponse($callback);
  }
  
}
*/