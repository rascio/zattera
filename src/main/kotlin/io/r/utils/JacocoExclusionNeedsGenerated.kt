package io.r.utils

/**
 * This annotation is used to mark classes that are excluded from Jacoco coverage.
 *
 * Jacoco requires the annotation to end with "Generated" to be excluded from coverage.
 */
@MustBeDocumented
@Retention(AnnotationRetention.RUNTIME)
@Target(
    AnnotationTarget.TYPE,
    AnnotationTarget.FILE,
    AnnotationTarget.CLASS,
    AnnotationTarget.FUNCTION
)
annotation class JacocoExclusionNeedsGenerated()
