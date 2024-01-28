module Program

open Expecto

[<EntryPoint>]
let main argv =
  runTestsInAssemblyWithCLIArgs [ Sequenced ] argv
