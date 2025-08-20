#include <jni.h>
#include <riscv_vector.h>
#include <stdio.h>
#include <stdlib.h>

/*
 * Class:     GeoFlink_utils_RvvDistanceCalculator
 * Method:    calculateDistances
 * Signature: (DD[D[D)[D
 */
JNIEXPORT jdoubleArray JNICALL Java_GeoFlink_utils_RvvDistanceCalculator_calculateDistances(
    JNIEnv *env,
    jclass cls,
    jdouble qx,
    jdouble qy,
    jdoubleArray streamX_j,
    jdoubleArray streamY_j) {

    // 1. Get pointers to the Java arrays
    jdouble *streamX = (*env)->GetDoubleArrayElements(env, streamX_j, NULL);
    jdouble *streamY = (*env)->GetDoubleArrayElements(env, streamY_j, NULL);
    if (streamX == NULL || streamY == NULL) {
        // Error handling: failed to get array elements
        return NULL;
    }

    jsize n = (*env)->GetArrayLength(env, streamX_j);

    // 2. Create the result array to be returned to Java
    jdoubleArray distances_j = (*env)->NewDoubleArray(env, n);
    jdouble *distances = (*env)->GetDoubleArrayElements(env, distances_j, NULL);
    if (distances == NULL) {
        // Clean up and return
        (*env)->ReleaseDoubleArrayElements(env, streamX_j, streamX, JNI_ABORT);
        (*env)->ReleaseDoubleArrayElements(env, streamY_j, streamY, JNI_ABORT);
        return NULL;
    }

    // 3. Main loop for RVV-based computation
    size_t gvl; // Group vector length
    for (size_t i = 0; i < n; i += gvl) {
        
        gvl = __riscv_vsetvl_e64m8(n - i); // Set vector length for this iteration

        // Load stream coordinates into vector registers
        vfloat64m8_t vec_x = __riscv_vle64_v_f64m8(&streamX[i], gvl);
        vfloat64m8_t vec_y = __riscv_vle64_v_f64m8(&streamY[i], gvl);

        // Calculate dx = streamX - qx
        vfloat64m8_t vec_dx = __riscv_vfsub_vf_f64m8(vec_x, qx, gvl);
        // Calculate dy = streamY - qy
        vfloat64m8_t vec_dy = __riscv_vfsub_vf_f64m8(vec_y, qy, gvl);

        // Calculate dx*dx
        vfloat64m8_t vec_dx_sq = __riscv_vfmul_vv_f64m8(vec_dx, vec_dx, gvl);
        // Calculate dy*dy
        vfloat64m8_t vec_dy_sq = __riscv_vfmul_vv_f64m8(vec_dy, vec_dy, gvl);

        // Calculate dist_sq = dx*dx + dy*dy
        vfloat64m8_t vec_dist_sq = __riscv_vfadd_vv_f64m8(vec_dx_sq, vec_dy_sq, gvl);

        // Calculate dist = sqrt(dist_sq)
        vfloat64m8_t vec_dist = __riscv_vfsqrt_v_f64m8(vec_dist_sq, gvl);

        // Store results back to the distances array
        __riscv_vse64_v_f64m8(&distances[i], vec_dist, gvl);
    }

    // 4. Release arrays and return the result
    (*env)->ReleaseDoubleArrayElements(env, streamX_j, streamX, JNI_ABORT); // JNI_ABORT: we don't need to copy back
    (*env)->ReleaseDoubleArrayElements(env, streamY_j, streamY, JNI_ABORT);
    (*env)->ReleaseDoubleArrayElements(env, distances_j, distances, 0); // 0: copy back and free buffer

    return distances_j;
}
