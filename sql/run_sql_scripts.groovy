def loader = this.class.classLoader.rootLoader
def scriptPath = this.class.protectionDomain.codeSource.location.path
def etlDir = new File(scriptPath).parentFile.parent
def sqlDir = new File(etlDir, 'sql')
loader.addURL(new File(etlDir, 'tm_etl.jar').toURI().toURL())

def cli = new CliBuilder(usage: 'tm_etl [options] [<data_dir>]')
cli.with {
    c longOpt: 'jdbc-url', args: 1, argName: '<jdbc url>', 'JDBC connection url'
    h longOpt: 'help', 'Show usage information'
    u longOpt: 'user', args: 1, argName: '<user>', 'Database user'
    p longOpt: 'password', args: 1, argName: '<password>', 'Database user password'
    'P' longOpt: 'procedures-only', 'Run only procedures scripts'
}

def opts = cli.parse(args)

if (opts?.h) {
    cli.usage()
    return
}

if (!opts?.'jdbc-url') {
    System.err.println("JDBC url isn't specified")
    System.exit(1)
    return
}

proceduresOnly = opts?.'procedures-only' as boolean


def Database = Class.forName('com.thomsonreuters.lsps.transmart.sql.Database', true, loader)
def database = Database.newInstance([
        db: [
                jdbcConnectionString: opts.'jdbc-url',
                username            : opts.'user',
                password            : opts.'password',
        ]
])

List<File> getScripts(File lstFile, List<String> defaultScripts) {
    List<String> scripts = lstFile.exists() ? lstFile.readLines() : defaultScripts
    return scripts.collect { new File(lstFile.parentFile, it) }
}

def runScripts(database, File dir, boolean proceduresOnly = false) {
    List<File> scripts = []
    if (!proceduresOnly) {
        scripts.addAll(getScripts(new File(dir, 'scripts.lst'), ['migrations.sql', 'permissions.sql']))
    }
    scripts.addAll(getScripts(new File(dir, 'procedures.lst'), ['procedures.sql']))
    scripts.each {
        println("Running script: ${it.name}...")
        database.runScript(it, true)
    }
    println("Completed: ${scripts.size()} scripts executed")
}

switch (database.databaseType.toString()) {
    case 'Postgres':
        runScripts(database, new File(sqlDir, 'postgres'), proceduresOnly)
        break;
    case 'Oracle':
        runScripts(database, new File(sqlDir, 'oracle'), proceduresOnly)
        break;
    default:
        System.err.println("Invalid database config")
        System.exit(1)
        return
}