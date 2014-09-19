/**
 * Date: 19.09.2014
 * Time: 13:14
 */
package com.thomsonreuters.lsps.transmart.util.transformations
import org.codehaus.groovy.GroovyBugError
import org.codehaus.groovy.ast.*
import org.codehaus.groovy.ast.expr.ArgumentListExpression
import org.codehaus.groovy.ast.expr.FieldExpression
import org.codehaus.groovy.ast.expr.MethodCallExpression
import org.codehaus.groovy.ast.expr.VariableExpression
import org.codehaus.groovy.ast.stmt.BlockStatement
import org.codehaus.groovy.ast.stmt.ExpressionStatement
import org.codehaus.groovy.ast.stmt.ReturnStatement
import org.codehaus.groovy.ast.stmt.Statement
import org.codehaus.groovy.control.CompilePhase
import org.codehaus.groovy.control.SourceUnit
import org.codehaus.groovy.control.messages.Message
import org.codehaus.groovy.transform.ASTTransformation
import org.codehaus.groovy.transform.GroovyASTTransformation
/**
 * Date: 19.09.2014
 * Time: 13:12
 */
@GroovyASTTransformation(phase = CompilePhase.CANONICALIZATION)
public class RequiresPrepareTransformation implements ASTTransformation {
    public void visit(ASTNode[] nodes, SourceUnit source) {
        if (nodes.length != 2 || !(nodes[0] instanceof AnnotationNode) || !(nodes[1] instanceof AnnotatedNode)) {
            throw new GroovyBugError("Internal error: expecting [AnnotationNode, AnnotatedNode] but got: " + Arrays.asList(nodes));
        }

        AnnotatedNode parent = (AnnotatedNode) nodes[1];
        BlockStatement statement;
        if (parent instanceof MethodNode) {
            statement = parent.code as BlockStatement
        } else if (parent instanceof FieldNode) {
            String getterName = "get" + parent.name.capitalize();
            MethodNode methodNode = parent.owner.getGetterMethod(getterName)
            if (methodNode == null) {
                statement = new BlockStatement([new ReturnStatement(new FieldExpression(parent))], new VariableScope())
                parent.owner.addMethod(new MethodNode(getterName, 0, parent.type, new Parameter[0], new ClassNode[0], statement))
            } else {
                statement = methodNode.code as BlockStatement
            }
        } else {
            source.getErrorCollector().addErrorAndContinue(Message.create("Unsupported element type", source))
            return;
        }
        List existingStatements = statement.getStatements()
        Statement prepareIfRequiredStatement = new ExpressionStatement(
                new MethodCallExpression(new VariableExpression("this"), "prepareIfRequired", new ArgumentListExpression()))
        existingStatements.add(0, prepareIfRequiredStatement)
    }
}

