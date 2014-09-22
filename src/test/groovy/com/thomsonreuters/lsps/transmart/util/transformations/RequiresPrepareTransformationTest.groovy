package com.thomsonreuters.lsps.transmart.util.transformations
/**
 * Date: 19.09.2014
 * Time: 15:00
 */
class RequiresPrepareTransformationTest extends GroovyTestCase {
    void testItTransformFields() {
        def source = '''
        import com.thomsonreuters.lsps.transmart.util.annotations.RequiresPrepare
        import com.thomsonreuters.lsps.transmart.util.PrepareIfRequired

        class Descendant extends Test {
        }

        @Mixin(PrepareIfRequired)
        abstract class Test {
            @RequiresPrepare
            String prop = "test"

            @RequiresPrepare
            String prop2

            String getProp() {
                return prop
            }

            void prepare() {
                prop2 = "test2"
            }
        }
        '''
        def clazz = new GroovyClassLoader().parseClass(source)
        def instance = clazz.newInstance()
        assertEquals(false, instance.prepared)
        assertEquals(instance.prop, "test")
        assertEquals(true, instance.prepared)
        assertEquals(clazz.newInstance().prop2, "test2")
    }
}
