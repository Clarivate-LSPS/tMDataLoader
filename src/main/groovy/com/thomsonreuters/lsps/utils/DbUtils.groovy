package com.thomsonreuters.lsps.utils

import com.thomsonreuters.lsps.db.core.Database
import groovy.sql.Sql
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import org.springframework.jdbc.support.lob.LobHandler

import java.nio.file.Files
import java.nio.file.Path
import java.sql.PreparedStatement
import java.sql.ResultSet

/**
 * Date: 11-May-16
 * Time: 13:56
 */
class DbUtils {
    public static <T> T withAutoCommit(Sql sql, boolean autoCommit, Closure<T> closure) {
        boolean savedAutoCommit = sql.connection.autoCommit
        try {
            sql.connection.autoCommit = autoCommit
            closure.call()
        } finally {
            sql.connection.autoCommit = savedAutoCommit
        }
    }

    public
    static <T> T withSelectResult(Map where = [:], Sql sql, String tableName, List<String> selectFields, boolean forUpdate = false, Closure<T> closure) {
        def params = []
        def sbFields = new StringBuilder()
        for (def fieldName : selectFields) {
            if (sbFields.size() > 0) {
                sbFields.append(', ')
            }
            sbFields.append(fieldName)
        }
        def sbCondition = new StringBuilder()
        for (def entry : where.entrySet()) {
            if (sbCondition.size() > 0) {
                sbFields.append(' and ')
            }
            sbCondition.append("$entry.key=?")
            params.add(entry.value)
        }
        def sbQuery = new StringBuilder("select $sbFields from $tableName")
        if (sbCondition.length() > 0) {
            sbQuery.append(" where ").append(sbCondition)
        }
        if (forUpdate) {
            sbQuery.append(" for update")
        }
        withQueryResult(sql, sbQuery.toString(), params, closure)
    }

    public static <T> T withQueryResult(Sql sql, String query, List<Object> params,
                                        @ClosureParams(value = SimpleType, options = ['java.sql.ResultSet']) Closure<T> closure) {
        def st = sql.connection.prepareStatement(query)
        try {
            int index = 0
            for (def param : params) {
                st.setObject(++index, param)
            }
            ResultSet resultSet = st.executeQuery()
            try {
                closure.call(resultSet)
            } finally {
                resultSet.close()
            }
        } finally {
            st.close()
        }
    }

    public
    static int insertRecord(Database database, Sql sql, String tableName, Map keyParams, Map<String, Object> params) {
        if (!keyParams) {
            throw new IllegalArgumentException("`keyParams` can't be empty")
        }
        LobHandler lobHandler = use(DatabaseExtensions) { database.lobHandler }
        ResourceUtils.withCloseableResources { usedResources ->
            withAutoCommit(sql, false) {
                def queryParams = []
                def values = new StringBuilder()
                def columns = new StringBuilder()
                for (def entry : keyParams.entrySet()) {
                    if (columns.length() > 0) {
                        columns.append(', ')
                        values.append(', ')
                    }
                    columns.append(entry.key)
                    values.append('?')
                    queryParams.add(entry.value)
                }

                for (def entry : params.entrySet()) {
                    columns.append(', ').append(entry.key)
                    values.append(', ?')
                    queryParams.add(entry.value)
                }

                PreparedStatement st = sql.connection.prepareStatement("insert into $tableName ($columns) values ($values)")
                int index = 0
                for (def value : queryParams) {
                    index++
                    if (value instanceof Path) {
                        def fis = value.newInputStream()
                        usedResources.add(fis)
                        long size = Files.size(value)
                        lobHandler.lobCreator.setBlobAsBinaryStream(st, index, fis, size <= Integer.MAX_VALUE ? (int) size : -1)
                    } else if (value instanceof InputStream) {
                        lobHandler.lobCreator.setBlobAsBinaryStream(st, index, value, -1)
                    } else {
                        st.setObject(index, value)
                    }
                }
                int result = st.executeUpdate()
                st.close()

                result
            }
        }
    }
}