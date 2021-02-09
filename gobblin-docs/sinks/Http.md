Table of Contents
------------------
[TOC]

# Introduction

Writing to a http based sink is done by sending a http or restful request and handling the response. Given
the endpoint uri, query parameters, and body, it is straightforward to construct a http request. The idea
is to build a writer that writes a http record, which contains those elements of a request. The writer
builds a http or rest request from multiple http records, sends the request with a client that knows the server,
and handles the response.

## Note
The old http write framework under [`AbstractHttpWriter`](https://github.com/apache/gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/writer/http/AbstractHttpWriter.java)
and [`AbstractHttpWriterBuilder`](https://github.com/apache/gobblin/blob/master/gobblin-core/src/main/java/org/apache/gobblin/writer/http/AbstractHttpWriterBuilder.java)
is deprecated (Deprecation date: 05/15/2018)! Use `AsyncHttpWriter` and `AsyncHttpWriterBuilder` instead

# Constructs
<p align="center">
    <img src=../../img/Http-Write.png>
</p>
<p style="text-align: center;"> Figure 1. Http write flow </p>

## `HttpOperation`
A http record is represented as a `HttpOperation` object. It has 4 fields.

| Field Name | Description | Example
|---|---|---|
| `keys` | Optional, a key/value map to interpolate the url template | ```{"memberId": "123"}``` |
| `queryParams` | Optional, a map from query parameter to its value| ```{"action": "update"}``` |
| `headers` | Optional, a map from header key to ts value | ```{"version": "2.0"}``` |
| `body` | Optional, the request body in string or json string format | ```"{\"email\": \"httpwrite@test.com\"}"``` |

Given an url template, ```http://www.test.com/profiles/${memberId}```, from job configuration, the resolved 
example request url with `keys` and `queryParams` information will be ```http://www.test.com/profiles/123?action=update```.

## `AsyncRequestBuilder`
An `AsyncRequestBuilder` builds an `AsyncRequest` from a collection of `HttpOperation` records. It could build one
request per record or batch multiple records into a single request. A builder is also responsible for
putting the `headers` and setting the `body` to the request.

## `HttpClient`
A `HttpClient` sends a request and returns a response. If necessary, it should setup the connection to the server, for
example, sending an authorization request to get access token. How authorization is done is per use case. Gobblin does
not provide general support for authorization yet.

## `ResponseHandler`
A `ResponseHandler` handles a response of a request. It returns a `ResponseStatus` object to the framework, which
would resend the request if it's a `SERVER_ERROR`.

# Build an asynchronous writer
`AsyncHttpWriterBuilder` is the base builder to build an asynchronous http writer. A specific writer can be created by 
providing the 3 major components: a `HttpClient`, a `AsyncRequestBuilder`, and a `ResponseHandler`.

Gobblin offers 2 implementations of async
http writers. As long as your write requirement can be expressed as a `HttpOperation` through a `Converter`, the
2 implementations should work with configurations.

## `AvroHttpWriterBuilder`
An `AvroHttpWriterBuilder` builds an `AsyncHttpWriter` on top of the [apache httpcomponents framework](https://hc.apache.org/), sending vanilla http request.
The 3 major components are:

  - `ApacheHttpClient`. It uses [`CloseableHttpClient`](https://github.com/apache/httpcomponents-client/blob/master/httpclient5/src/main/java/org/apache/hc/client5/http/impl/classic/CloseableHttpClient.java) to 
  send [`HttpUriRequest`](https://github.com/apache/httpcomponents-client/blob/master/httpclient5/src/main/java/org/apache/hc/client5/http/classic/methods/HttpUriRequest.java)
  and receive [`CloseableHttpResponse`](https://github.com/apache/httpcomponents-client/blob/master/httpclient5/src/main/java/org/apache/hc/client5/http/impl/classic/CloseableHttpResponse.java)
  - `ApacheHttpRequestBuilder`. It builds a `ApacheHttpRequest`, which is an `AsyncRequest` that wraps the `HttpUriRequest`, from one `HttpOperation`
  - `ApacheHttpResponseHandler`. It handles a `HttpResponse`

Configurations for the builder are:

| Configuration | Description | Example
|---|---|---|
| `gobblin.writer.http.urlTemplate` | Required, the url template(schema and port included), together with `keys` and `queryParams`, to be resolved to request url | ```http://www.test.com/profiles/${memberId}``` |
| `gobblin.writer.http.verb` | Required, [http verbs](http://www.restapitutorial.com/lessons/httpmethods.html) | get, update, delete, etc |
| `gobblin.writer.http.errorCodeWhitelist` | Optional, http error codes allowed to pass through | 404, 500, etc. No error code is allowed by default |
| `gobblin.writer.http.maxAttempts` | Optional, max number of attempts including initial send | Default is 3 |
| `gobblin.writer.http.contentType` | Optional, content type of the request body | ```"application/json"```, which is the default value |

## `R2RestWriterBuilder`
A `R2RestWriterBuilder` builds an `AsyncHttpWriter` on top of [restli r2 framework](https://github.com/linkedin/rest.li/wiki/Request---Response-API-(R2)), sending
rest request. The 3 major components are:

  - `R2Client`. It uses a R2 [`Client`](https://github.com/linkedin/rest.li/blob/master/r2-core/src/main/java/com/linkedin/r2/transport/common/Client.java) to
  send [`RestRequest`](https://github.com/linkedin/rest.li/blob/master/r2-core/src/main/java/com/linkedin/r2/message/rest/RestRequest.java) and
  receive [`RestResponse`](https://github.com/linkedin/rest.li/blob/master/r2-core/src/main/java/com/linkedin/r2/message/rest/RestResponse.java)
  - `R2RestRequestBuilder`. It builds a `R2Request`, which is an `AsyncRequest` that wraps the `RestRequest`, from one `HttpOperation`
  - `R2RestResponseHandler`. It handles a `RestResponse`
  
 `R2RestWriterBuilder` has [d2](https://github.com/linkedin/rest.li/wiki/Dynamic-Discovery) and ssl support. Configurations(`(d2.)` part should be added in d2 mode) for the builder are:
 
 | Configuration | Description | Example
 |---|---|---|
 | `gobblin.writer.http.urlTemplate` | Required, the url template(schema and port included), together with `keys` and `queryParams`, to be resolved to request url. If the schema is `d2`, d2 is enabled | ```http://www.test.com/profiles/${memberId}``` |
 | `gobblin.writer.http.verb` | Required, [rest(rest.li) verbs](https://github.com/linkedin/rest.li/wiki/Rest.li-User-Guide#resource-methods) | get, update, put, delete, etc |
 | `gobblin.writer.http.maxAttempts` | Optional, max number of attempts including initial send | Default is 3 |
 | `gobblin.writer.http.errorCodeWhitelist` | Optional, http error codes allowed to pass through | 404, 500, etc. No error code is allowed by default |
 | `gobblin.writer.http.d2.zkHosts`| Required for d2, the zookeeper address | |
 | `gobblin.writer.http.(d2.)ssl`| Optional, enable ssl | Default is false |
 | `gobblin.writer.http.(d2.)keyStoreFilePath`| Required for ssl | /tmp/identity.p12 |
 | `gobblin.writer.http.(d2.)keyStoreType`| Required for ssl | PKCS12 |
 | `gobblin.writer.http.(d2.)keyStorePassword`| Required for ssl | |
 | `gobblin.writer.http.(d2.)trustStoreFilePath`| Required for ssl | |
 | `gobblin.writer.http.(d2.)trustStorePassword`| Required for ssl | |
 | `gobblin.writer.http.protocolVersion` | Optional, protocol version of rest.li | ```2.0.0```, which is the default value |

`R2RestWriterBuilder` isn't ingegrated with `PasswordManager` to process encrypted passwords yet. The task is tracked as https://issues.apache.org/jira/browse/GOBBLIN-487

# Build a synchronous writer
The idea is to reuse an asynchronous writer to build its synchronous version. The technical difference between them
is the size of outstanding writes. Set `gobblin.writer.http.maxOutstandingWrites` to be `1`(default value is `1000`) to make a synchronous writer
