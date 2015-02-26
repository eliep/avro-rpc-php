<?php

require_once __DIR__."/../../vendor/autoload.php";

use Avro\Examples\Protocol\Fr\V3d\Avro\MailProtocol;

$datum = array(
  array("to" => "me", "from" => "me", "body" => "yep"),
  array("to" => "me", "from" => "me", "body" => "yep"),
  array("to" => "me", "from" => "me", "body" => "yep")
);

$protocol = MailProtocol::getClient('localhost', 1421);
foreach ($datum as $data) {
  $response = $protocol->send($data);
  echo json_encode($response)."\n";
}