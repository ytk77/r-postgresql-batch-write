suppressMessages(library(RPostgreSQL))
suppressMessages(library(dplyr))
suppressMessages(library(yaml))


fnc_init_Postgre_conn <- function()
{
    dbcnf <- yaml.load_file("./config/db_conn.conf")
    
    drv <- dbDriver("PostgreSQL")
    conn <- dbConnect(drv,
                      host = dbcnf$host, port = dfcnf$port,
                      user = dbcnf$user, password = dbcnf$password,
                      dbname = dbcnf$dbname)
    
    return(conn)
}


pgsql.write.table <- function(tbl_name, DAT, keys)
{
    conn = fnc_init_Postgre_conn()
    
    ## generate hash value from primary keys
    qstr <- paste0("D2 <- DAT %>% dplyr::mutate(pkey = paste0(",
                   paste(keys, collapse = ", '_', "), ") )")
    eval( parse(text = qstr))

    if (!dbExistsTable(conn, tbl_name)) {  ## save to a new table
        dbWriteTable(conn, tbl_name, D2)
        ## alter table to add primary key
        qstr = paste('ALTER TABLE', tbl_name, 'ADD PRIMARY KEY ("pkey")')
        res <- dbSendQuery(conn, qstr)
    } else {
        ## exclude existing records in DB
        qstr <- paste0("SELECT pkey FROM ", tbl_name)
        res <- dbSendQuery(conn, qstr)
        R1 <- fetch(res, n=-1)
        D3 <- D2 %>% anti_join(R1)
        dbWriteTable(conn, tbl_name, D3, append=TRUE)
    }
    
    dbDisconnect(conn)
}
