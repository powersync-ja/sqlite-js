
    /*
    **  --sleep N
    **
    ** Pause for N milliseconds
    */
    /*
    **   --exit N
    **
    ** Exit this process.  If N>0 then exit without shutting down
    ** SQLite.  (In other words, simulate a crash.)
    */
    /*
    **   --testcase NAME
    **
    ** Begin a new test case.  Announce in the log that the test case
    ** has begun.
    */

    /*
    **   --finish
    **
    ** Mark the current task as having finished, even if it is not.
    ** This can be used in conjunction with --exit to simulate a crash.
    */
    /*
    **  --reset
    **
    ** Reset accumulated results back to an empty string
    */

    /*
    **  --match ANSWER...
    **
    ** Check to see if output matches ANSWER.  Report an error if not.
    */
    /*
    **  --glob ANSWER...
    **  --notglob ANSWER....
    **
    ** Check to see if output does or does not match the glob pattern
    ** ANSWER.
    */
    /*
    **  --output
    **
    ** Output the result of the previous SQL.
    */

    /*
    **  --source FILENAME
    **
    ** Run a subscript from a separate file.
    */
    /*
    **  --print MESSAGE....
    **
    ** Output the remainder of the line to the log file
    */

    /*
    **  --if EXPR
    **
    ** Skip forward to the next matching --endif or --else if EXPR is false.
    */
    /*
    **  --else
    **
    ** This command can only be encountered if currently inside an --if that
    ** is true.  Skip forward to the next matching --endif.
    */
    /*
    **  --endif
    **
    ** This command can only be encountered if currently inside an --if that
    ** is true or an --else of a false if.  This is a no-op.
    */

    /*
    **  --start CLIENT
    **
    ** Start up the given client.
    */
    /*
    **  --wait CLIENT TIMEOUT
    **
    ** Wait until all tasks complete for the given client.  If CLIENT is
    ** "all" then wait for all clients to complete.  Wait no longer than
    ** TIMEOUT milliseconds (default 10,000)
    */

    /*
    **  --task CLIENT
    **     <task-content-here>
    **  --end
    **
    ** Assign work to a client.  Start the client if it is not running
    ** already.
    */