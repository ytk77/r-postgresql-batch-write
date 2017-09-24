suppressMessages(library(RPostgreSQL))
suppressMessages(library(dplyr))
suppressMessages(library(yaml))


fnc_init_Postgre_conn <- function()
{
    dbcnf <- yaml.load_file("./config/db_conn.conf")
    
    drv <- dbDriver("PostgreSQL")
    conn <- dbConnect(drv,
                      host = dbcnf$host, port = df=dbcnf$port,
                      user = dbcnf$user, password = dbcnf$password,
                      dbname = dbcnf$dbname)
    
    return(conn)
}


pgsql.write.table.per.rec <- function(tbl_name, DAT, keys)
{
    tbl_name = tolower(tbl_name)
    names(DAT) = tolower(  names(DAT) )
    conn = fnc_init_Postgre_conn()
    
    if (!dbExistsTable(conn, tbl_name)) {  ## save to a new table
        d2 = head(DAT, 1)
        dbWriteTable(conn, tbl_name, d2)
        ## alter table to add primary key
        qstr = paste0('ALTER TABLE ', tbl_name, ' ADD PRIMARY KEY (',
                      paste0(keys, collapse=','), ')')
        res <- dbSendQuery(conn, qstr)
        
        D3 <- DAT[2:nrow(DAT), ]
        try( dbWriteTable(conn, tbl_name, D3, append=TRUE) )
        
    } else {
        ## try write all first
        r1 = try( dbWriteTable(conn, tbl_name, DAT, append=TRUE) )
        ## if error, write records one by one
        if (class(r1)=="try-error") {
            # write to table per record
            for (ii in 1:nrow(DAT))
            {
                d3 <- DAT[ii, ]
                try( dbWriteTable(conn, tbl_name, d3, append=TRUE) )
            }
        }
        
    }
    
    dbDisconnect(conn)
}
