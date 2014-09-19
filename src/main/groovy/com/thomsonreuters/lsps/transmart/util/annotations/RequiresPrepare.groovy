package com.thomsonreuters.lsps.transmart.util.annotations

import org.codehaus.groovy.transform.GroovyASTTransformationClass

import java.lang.annotation.ElementType
import java.lang.annotation.Retention
import java.lang.annotation.RetentionPolicy
import java.lang.annotation.Target

/**
 * Date: 19.09.2014
 * Time: 13:15
 */

@Retention(RetentionPolicy.SOURCE)
@Target([ElementType.METHOD, ElementType.FIELD])
@GroovyASTTransformationClass(["com.thomsonreuters.lsps.transmart.util.transformations.RequiresPrepareTransformation"])
@interface RequiresPrepare {

}