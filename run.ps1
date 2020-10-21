$method, $rest = $args

Try {
    cd "$method"
    sbt "run -c `"../config.json`" $($rest)"
}

Finally {
    cd $PSScriptRoot
}
