/**
 * Date: 19.09.2014
 * Time: 13:14
 */
package com.thomsonreuters.lsps.transmart.util.transformations
import com.thomsonreuters.lsps.transmart.util.annotations.RequiresPrepare
import org.codehaus.groovy.ast.ASTNode
import org.codehaus.groovy.ast.ClassNode
import org.codehaus.groovy.ast.MethodNode
import org.codehaus.groovy.ast.expr.ArgumentListExpression
import org.codehaus.groovy.ast.expr.MethodCallExpression
import org.codehaus.groovy.ast.expr.VariableExpression
import org.codehaus.groovy.ast.stmt.BlockStatement
import org.codehaus.groovy.ast.stmt.ExpressionStatement
import org.codehaus.groovy.ast.stmt.Statement
import org.codehaus.groovy.control.CompilePhase
import org.codehaus.groovy.control.SourceUnit
import org.codehaus.groovy.transform.ASTTransformation
import org.codehaus.groovy.transform.GroovyASTTransformation
/**
 * Date: 19.09.2014
 * Time: 13:12
 */
@GroovyASTTransformation(phase= CompilePhase.SEMANTIC_ANALYSIS)
public class RequiresPrepareTransformation implements ASTTransformation {
    public void visit(ASTNode[] nodes, SourceUnit sourceUnit) {
        List methods = nodes.findAll { it instanceof MethodNode }
        methods.findAll { MethodNode method ->
            method.getAnnotations(new ClassNode(RequiresPrepare))
        }.each { MethodNode method ->
            List existingStatements = (method.getCode() as BlockStatement).getStatements()
            Statement prepareIfRequiredStatement = new ExpressionStatement(
                    new MethodCallExpression(new VariableExpression("this"), "prepareIfRequired", new ArgumentListExpression()))
            existingStatements.add(0, prepareIfRequiredStatement)
        }
    }
}

