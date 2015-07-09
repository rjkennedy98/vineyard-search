xquery version "1.0-ml";
module namespace wm = "https://github.com/rjkennedy98/vineyard-search/mlcp";
declare namespace w = "https://github.com/rjkennedy98/vineyard-search";


declare function wm:get-elements($root-qname as xs:QName, $element-qname as xs:QName, $text) {
    element {$root-qname } {
       if (fn:string-length($text) gt 0) then
         let $tokens  := fn:tokenize($text, ";")
         return for $token in $tokens where fn:string-length($text) gt 0
            return element { $element-qname } { fn:normalize-space($token)}
      else ()
    }
 
};

declare function wm:get-year($wine-name) {
   let $year := (
    for $token in fn:tokenize($wine-name, " ")
      return if (fn:matches($token, "^\d\d\d\d$")) then $token else ()
   )[1]
   return if ($year) then <w:year>{$year}</w:year> else ()
};


declare function wm:create-xml($identifier, $input-xml) {
  let $dXml := <root>{
   for $node in $input-xml/*/*
     return if (fn:string-length($node/fn:string() ) gt 0 ) 
     then element { xs:QName( fn:local-name($node) ) } { xdmp:base64-decode($node/fn:string())}
     else ()
  
  }</root>
  
  return <w:wineListing>
    <w:wineIdentifier>{$identifier}</w:wineIdentifier>
    <w:lastModified>{fn:current-dateTime()}</w:lastModified>
    <w:creationDatetime>{fn:current-dateTime()}</w:creationDatetime>
    <w:wineName>{fn:normalize-space($dXml/wine_name/fn:string()) }</w:wineName>{
    wm:get-year($dXml/wine_name/fn:string()) 
    }
    <w:rating>{fn:normalize-space($dXml/rating/fn:string())}</w:rating>{
      wm:get-elements(xs:QName("w:varietals"), xs:QName("w:varietal"), $dXml/varietal/fn:string() ),
      wm:get-elements(xs:QName("w:appellations"), xs:QName("w:appellation"), $dXml/appellation/fn:string() )
    }
    <w:winery>{fn:normalize-space($dXml/winery/fn:string())}</w:winery>
    <w:alcoholPercentage>{fn:normalize-space($dXml/alcohol/fn:string())}</w:alcoholPercentage>
    <w:caseProduction>{fn:normalize-space($dXml/production/fn:string() )}</w:caseProduction>
    <w:bottleSize>{fn:normalize-space($dXml/bottle_size/fn:string())}</w:bottleSize>
    {
    wm:get-elements(xs:QName("w:categories"), xs:QName("w:category"), $dXml/category/fn:string() )
    }
    <w:importer>{fn:normalize-space($dXml/w:importer/fn:string())}</w:importer>
    <w:reviewText>{ $dXml/review_/fn:string()}</w:reviewText>
  </w:wineListing>
};


declare function wm:transform($content as map:map, $context as map:map ) as map:map* {
  let $attr-value := 
    (map:get($context, "transform_param"), "UNDEFINED")[1]
  let $the-doc := map:get($content, "value")
  return
    if (fn:empty($the-doc/element()))
    then $content
    else
      let $identifier := xdmp:hash64(xdmp:quote($the-doc))
      let $uri := "/vineyard/wine/" || $identifier || ".xml"
      let $root := $the-doc/*
      return (
        map:put($content, "value",
          document {
             wm:create-xml($identifier, $the-doc)
          }
        ), 
        map:put($content, "uri", $uri),
        $content
      )
};
