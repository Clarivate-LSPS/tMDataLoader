package com.thomsonreuters.lsps.transmart.util.transformations
/**
 * Date: 19.09.2014
 * Time: 15:00
 */
class RequiresPrepareTransformationTest extends GroovyTestCase {
    void testItTransformFields() {
        def source = '''
        import com.thomsonreuters.lsps.transmart.util.annotations.RequiresPrepare

        class Test {
            @RequiresPrepare
            String prop = "test"

            @RequiresPrepare
            String prop2

            boolean prepared;

            String getProp() {
                return prop
            }

            void prepareIfRequired() {
                prop2 = "test2"
                prepared = true;
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
