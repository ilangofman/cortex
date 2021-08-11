package api

import (
	"net/http"

	"github.com/weaveworks/common/middleware"

	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/cortexproject/cortex/pkg/tenant"
)

// middleware for setting cache gen header to let consumer of response know all previous responses could be invalid due to delete operation
func getHTTPCacheGenNumberHeaderSetterMiddleware(cacheGenNumbersLoader queryrange.CacheGenNumberLoader) middleware.Interface {
	return middleware.Func(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tenantIDs, err := tenant.TenantIDs(r.Context())
			if err != nil {
				http.Error(w, err.Error(), http.StatusUnauthorized)
				return
			}

			cacheGenNumber := cacheGenNumbersLoader.GetResultsCacheGenNumber(r.Context(), tenantIDs)

			w.Header().Set(queryrange.ResultsCacheGenNumberHeaderName, cacheGenNumber)
			next.ServeHTTP(w, r)
		})
	})
}
