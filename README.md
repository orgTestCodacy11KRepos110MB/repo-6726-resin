# Siresin

16-bit wide vector-space model search engine with HTTP API and pluggable read/write pipelines.

## Platform

.NET Core 2.0.

## Documentation

### Vector-space model

To provide full-text search across your documents words and phrases are mapped to a 65k dimensional vector-space that form clusters of syntactically similar "bag-of-chars". On disk and in-memory this model is represented as a [binary tree](src/Sir.Store/VectorNode.cs).

### HTTP API

Send and recieve data in any format using any query language through pluggable read/write pipelines. [Read more](src/Sir.HttpServer/README.md).