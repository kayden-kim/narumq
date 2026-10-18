hs = {}

if obj.status == nil or obj.status.phase == nil then
  hs.status = "Progressing"
  hs.message = "rollout status has not been initialized"
  return hs
end

if obj.status.phase == "Completed" then
  hs.status = "Healthy"
  hs.message = "all broker ordinals reached the target release profile"
elseif obj.status.phase == "Failed" or obj.status.phase == "Aborted" then
  hs.status = "Degraded"
  hs.message = obj.status.failureReason or obj.status.phase
else
  hs.status = "Progressing"
  hs.message = obj.status.failureReason or obj.status.phase
end

return hs
