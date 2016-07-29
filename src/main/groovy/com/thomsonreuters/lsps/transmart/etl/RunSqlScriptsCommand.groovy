package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.db.core.Database
import com.thomsonreuters.lsps.db.core.DatabaseType

/**
 * Date: 17-Dec-15
 * Time: 13:17
 */
class RunSqlScriptsCommand {
    private class Scripts {
        List<File> dbaScripts = []
        List<File> userScripts = []
    }

    def runScripts(Database database, Scripts scripts, String dbaUser, String dbaPassword) {
        if (scripts.dbaScripts) {
            if (!dbaUser || !dbaPassword) {
                throw new RuntimeException("You should provide DBA credentials to run these scripts")
            }
            def dbaDatabase = database.withCredentials(dbaUser, dbaPassword)
            for (File script : scripts.dbaScripts) {
                println("Running script as dba: ${script.name}...")
                dbaDatabase.runScript(script, true)
            }
        }
        for (File script : scripts.userScripts) {
            println("Running script: ${script.name}...")
            database.runScript(script, true)
        }
        println("Completed: ${scripts.dbaScripts.size() + scripts.userScripts.size()} scripts executed")
    }

    private Scripts collectScripts(Database database, boolean proceduresOnly) {
        def scripts = new Scripts()
        def etlDir = new File('.')
        def sqlDir = new File(etlDir, 'sql')
        switch (database.databaseType) {
            case DatabaseType.Postgres:
                def scriptsDir = new File(sqlDir, 'postgres')
                if (!proceduresOnly) {
                    scripts.dbaScripts.addAll(['migrations.sql', 'permissions.sql'].collect {
                        new File(scriptsDir, it)
                    })
                }
                scripts.dbaScripts.addAll(['procedures.sql'].collect { new File(scriptsDir, it) })
                break
            case DatabaseType.Oracle:
                def scriptsDir = new File(sqlDir, 'oracle')
                if (!proceduresOnly) {
                    scripts.dbaScripts.add(new File(scriptsDir, 'run_as_dba.sql'))
                }
                scripts.userScripts.add(new File(scriptsDir, 'run_as_tm_dataloader.sql'))
                break
            default:
                System.err.println("Invalid database config")
                System.exit(1)
                return
        }
        scripts
    }

    public void run(Database database, String dbaUser, String dbaPassword, boolean proceduresOnly) {
        runScripts(database, collectScripts(database, proceduresOnly), dbaUser, dbaPassword)
    }

    public static void main(String[] args) {
        def cli = new CliBuilder(usage: 'run_sql_scripts [options]')
        cli.with {
            h longOpt: 'help', 'Show usage information'
            u longOpt: 'dba-user', args: 1, argName: '<user>', 'Database admin user'
            p longOpt: 'dba-password', args: 1, argName: '<password>', 'Database admin user password'
            'P' longOpt: 'procedures-only', 'Run only procedures scripts'
            c longOpt: 'config', args: 1, argName: '<config_path>', 'Config file path. Default: ~/.tm_etl/Config.groovy'
        }

        def opts = cli.parse(args)

        if (opts?.h) {
            cli.usage()
            return
        }

        def configFileName
        if (opts.config) {
            configFileName = opts.config
            println ">>> USING CONFIG: ${configFileName}"
        } else {
            configFileName = System.getProperty('user.home') + File.separator + '.tm_etl' + File.separator + 'Config.groovy'
        }

        def config
        try {
            config = new ConfigSlurper().parse(
                    new File(configFileName).toURI().toURL()
            )
        }
        catch (e) {
            println "Error processing config: ${e}\n"
            return
        }

        def proceduresOnly = opts?.'procedures-only' as boolean
        def database = TransmartDatabaseFactory.newDatabase(config)
        new RunSqlScriptsCommand().run(database, opts.'dba-user' ?: null, opts.'dba-password' ?: null, proceduresOnly)
    }
}
