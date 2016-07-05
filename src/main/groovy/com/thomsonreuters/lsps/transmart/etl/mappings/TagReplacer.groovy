package com.thomsonreuters.lsps.transmart.etl.mappings

import com.thomsonreuters.lsps.transmart.etl.DataProcessingException
import com.thomsonreuters.lsps.transmart.etl.mappings.ClinicalDataMapping.FileMapping

import java.util.regex.Pattern

/**
 * Date: 11-Apr-16
 * Time: 14:26
 */
class TagReplacer {
    protected static final Pattern RE_PLUS = Pattern.compile(/\+/)
    private static final Pattern tagPattern = Pattern.compile(/\$\$(\{[^}]+\}|[^+]+)/)

    Map tagMapping = [:]
    TagNameMangler tagNameMangler = new TagNameMangler()

    private static class TagNameMangler {
        private static final char firstChar = 'A'.charAt(0)
        private static final char lastChar = 'Z'.charAt(0)
        private static final char positionSize = lastChar - firstChar + 1

        private int counter
        Map<String, String> name2mangledName = [:]

        String getMangledName(String name) {
            String mangledName = name2mangledName.get(name)
            if (mangledName == null) {
                name2mangledName.put(name, mangledName = nextMangledName())
            }
            mangledName
        }

        private String nextMangledName() {
            //noinspection GroovyAssignabilityCheck
            String name = String.valueOf((char) (firstChar + counter % positionSize)).toString()
            if (counter >= positionSize) {
                name = String.valueOf((int) (counter / positionSize)) + name
            }
            counter++
            name
        }
    }

    TagReplacer(Map tagMapping) {
        this.tagMapping = tagMapping
    }

    public static void main(String[] args) {
        def mangler = new TagNameMangler()
        for (int i = 0; i < 1000; i++) {
            println mangler.nextMangledName()
        }
    }

    static TagReplacer fromFileMapping(FileMapping fileMapping) {
        def _DATA = fileMapping._DATA
        Map tagToColumn = _DATA.collectEntries {
            [(it.DATA_LABEL): it.COLUMN]
        }
        tagToColumn['STUDY_ID'] = fileMapping.STUDY_ID
        tagToColumn['SITE_ID'] = fileMapping.SITE_ID
        tagToColumn['SUBJ_ID'] = fileMapping.SUBJ_ID
        tagToColumn['SAMPLE_ID'] = fileMapping.SAMPLE_ID

        for (def v : _DATA) {
            def categoryCd = v.CATEGORY_CD
            //noinspection GroovyAssignabilityCheck
            for (List<String> groups : tagPattern.matcher(categoryCd)) {
                def tag = groups[1] as String
                if (tag.startsWith('{')) {
                    if (!tag.endsWith('}')) {
                        throw new DataProcessingException("$fileMapping.fileName: Non closed tag '${tag[1..-1]}' found in '$categoryCd'")
                    }
                    tag = tag[1..<-1]
                }
                if (!tagToColumn.containsKey(tag)) {
                    throw new DataProcessingException("$fileMapping.fileName: cat_cd '$categoryCd' contains not-existing tag: '$tag'")
                }
            }
        }
        new TagReplacer(tagToColumn)
    }

    String replaceTags(String value, String[] cols) {
        if (value.contains('$$')) {
            boolean hasEmptyTags = false
            value = value.replaceAll(tagPattern) { String match, String name ->
                boolean partialMatch = name.startsWith('{')
                if (partialMatch) {
                    name = name[1..<-1]
                }
                def tagValue = cols[tagMapping[name] as Integer] as String
                if (!tagValue) {
                    hasEmptyTags = true
                    return match
                }

                tagValue = tagValue.replaceAll(RE_PLUS, '(plus)')
                if (partialMatch)
                    tagValue = "{$tagValue}"

                '$$' + tagNameMangler.getMangledName(name) + tagValue
            }

            //ignore record without tags value
            if (hasEmptyTags)
                return null
        }
        value
    }
}
