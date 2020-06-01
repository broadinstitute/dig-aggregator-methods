$method, $rest = $args
cd "$method"
sbt "run -c `"../config.json`" $($rest)"
cd $PSScriptRoot
