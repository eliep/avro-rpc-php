<?php

namespace Avro\Generator;

use Avro\RPC\RpcProtocolHelper;

class ProtocolGenerator {
  
    private $protocol_tpl =<<< PT
<?php

namespace PT_NAMESPACE;

use Avro\RPC\RpcProtocol;

class PT_CLASSNAME extends RpcProtocol {
  
  private \$jsonProtocol =
PT_JSON

  public function getJsonProtocol() { return \$this->jsonProtocol; }
  
PT_CLIENT_FUNCTIONS

PT_SERVER_FUNCTIONS
  
}
PT;

    private $client_function_tpl = <<<CFT
    
  public function CFT_NAME(CFT_PARAMS) {
    return \$this->genericRequest(array(CFT_PARAMS));
  }
    
CFT;
  
    private $server_function_tpl = <<<SFT
    
  public function SFT_NAMEImpl(\$callback) {
    return \$this->genericResponse(\$callback);
  }
    
SFT;
  
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
    $protocol_helper = new RpcProtocolHelper($protocol_json);
    $protocol = $protocol_helper->getProtocol();
    
    $filename = $this->getFilename($protocol);
    $subdirectory = $this->getSubdirectory($protocol);
    $protocol_tpl = $this->generate($protocol, $protocol_json, $namespace_prefix);
    if (!file_exists($output_folder.$subdirectory))
      mkdir($output_folder.$subdirectory, 0755, true);
    file_put_contents($output_folder.$subdirectory."/".$filename, $protocol_tpl);
  }
  
  public function generate($protocol, $protocol_json, $namespace_prefix) {
    $namespace = $this->getNamespace($protocol, $namespace_prefix);
    $this->protocol_tpl = str_replace("PT_NAMESPACE", $namespace, $this->protocol_tpl);
    
    $classname = $this->getClassname($protocol);
    $this->protocol_tpl = str_replace("PT_CLASSNAME", $classname, $this->protocol_tpl);
    
    $json = $this->getJson($protocol_json);
    $this->protocol_tpl = str_replace("PT_JSON", $json, $this->protocol_tpl);
    
    $client_functions = $this->getClientFunctions($protocol);
    $this->protocol_tpl = str_replace("PT_CLIENT_FUNCTIONS", $client_functions, $this->protocol_tpl);
    
    $server_functions = $this->getServerFunctions($protocol);
    $this->protocol_tpl = str_replace("PT_SERVER_FUNCTIONS", $server_functions, $this->protocol_tpl);
    
    return $this->protocol_tpl;
  }
  
  public function getNamespace($protocol, $namespace_prefix = null) {
    $namespace_token = explode(".", $protocol->namespace);
    array_walk($namespace_token, function(&$token) { $token = ucfirst($token); });
    return (!is_null($namespace_prefix)) ? $namespace_prefix."\\". implode("\\", $namespace_token) : implode("\\", $namespace_token);
  }
  
  public function getClassname($protocol) {
    return $protocol->name."Protocol";
  }
  
  public function getFilename($protocol) {
    return $this->getClassname($protocol).".php";
  }
  
  public function getSubdirectory($protocol) {
    return "/".str_replace("\\", "/", $this->getNamespace($protocol));
  }
  
  
  public function getJson($protocol_json) {
    return $json = "<<<PROTO\n".json_encode(json_decode($protocol_json))."\nPROTO;\n";
  }
  
  public function getClientFunctions($protocol) {
    $client_functions = array();
    
    $messages = $protocol->messages;
    foreach ($messages as $msg_name => $msg_def) {
      $client_functions[$msg_name] = array();
      foreach ($msg_def->request->fields() as $field) {
        //echo $field->name()."/".$field->type(). "\n";
        $client_functions[$msg_name][] = "$".$field->name();
      }
      $client_functions[$msg_name] = str_replace("CFT_PARAMS", implode(", ", $client_functions[$msg_name]), $this->client_function_tpl);
      $client_functions[$msg_name] = str_replace("CFT_NAME", $msg_name, $client_functions[$msg_name]);
      //echo $msg_def->response->fullname()."/".$msg_def->response->qualified_name()."/".$msg_def->response->type()."\n";
    }
    return implode("\n", $client_functions);
  }
  
  
  public function getServerFunctions($protocol) {
    $server_functions = array();
    
    $messages = $protocol->messages;
    foreach ($messages as $msg_name => $msg_def) 
      $server_functions[$msg_name] = str_replace("SFT_NAME", $msg_name, $this->server_function_tpl);

    return implode("\n", $server_functions);
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