<?php
require_once __DIR__."/../../vendor/autoload.php";

use Avro\Examples\Protocol\Fr\V3d\Avro\MailProtocol;


$protocol = MailProtocol::getServer('localhost', 1421);
$protocol->sendImpl(function($params) {
  $msg = $params[0];
  return array("body"=>"toto");
});
