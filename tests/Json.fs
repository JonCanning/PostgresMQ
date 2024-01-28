module Json

open System.Text.Json
open System.Text.Json.Serialization
open System

let options =
  let options =
    JsonSerializerOptions(
      DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
      PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
      DictionaryKeyPolicy = JsonNamingPolicy.CamelCase,
      PropertyNameCaseInsensitive = true
    )

  JsonFSharpConverter(
    JsonUnionEncoding.FSharpLuLike,
    unionTagCaseInsensitive = true,
    unionTagNamingPolicy = JsonNamingPolicy.CamelCase,
    unionFieldNamingPolicy = JsonNamingPolicy.CamelCase,
    allowNullFields = true
  )
  |> options.Converters.Add

  JsonStringEnumConverter() |> options.Converters.Add
  options
