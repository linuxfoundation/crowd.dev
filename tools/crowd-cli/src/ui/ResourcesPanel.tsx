import {memo} from 'react'
import {Box, Text} from 'ink'
import {ContainerInfo} from '../lib/docker.js'
import {ImageSize, VolumeSize, DockerDiskUsage} from '../lib/resources.js'

interface Props {
  statuses: Record<string, ContainerInfo>
  imageSizes: Record<string, ImageSize>
  volumeSizes: VolumeSize[] | null
  dockerDisk: DockerDiskUsage | null
}

export const ResourcesPanel = memo(function ResourcesPanel({statuses, imageSizes, volumeSizes, dockerDisk}: Props) {
  const imageEntries = Object.entries(statuses)
    .flatMap(([svc, info]) => {
      const img = imageSizes[info.image]
      return img ? [{svc, formatted: img.formatted, bytes: img.bytes}] : []
    })
    .sort((a, b) => b.bytes - a.bytes)

  const volEntries = volumeSizes === null
    ? null
    : [...volumeSizes].sort((a, b) => b.bytes - a.bytes)

  const imageColW = imageEntries.length > 0
    ? Math.max(...imageEntries.map(e => e.svc.length + 2 + e.formatted.length)) + 2
    : 16

  const volColW = volEntries && volEntries.length > 0
    ? Math.max(...volEntries.map(v => 2 + v.displayName.length + 2 + v.size.length)) + 2
    : 16

  return (
    <Box flexDirection="column" marginTop={1}>
      <Box gap={1}>
        <Text color="gray" bold>IMAGES</Text>
        {imageEntries.length === 0 && <Text color="gray">—</Text>}
      </Box>
      {imageEntries.length > 0 && (
        <Box flexWrap="wrap" paddingLeft={1}>
          {imageEntries.map(({svc, formatted}) => (
            <Box key={svc} width={imageColW}>
              <Text color="gray">{svc}: </Text>
              <Text color="gray" bold>{formatted}</Text>
            </Box>
          ))}
        </Box>
      )}

      <Box gap={1} marginTop={1}>
        <Text color="gray" bold>VOLUMES</Text>
        {volEntries === null && <Text color="gray">calculating…</Text>}
        {volEntries !== null && volEntries.length === 0 && <Text color="gray">—</Text>}
      </Box>
      {volEntries !== null && volEntries.length > 0 && (
        <Box flexWrap="wrap" paddingLeft={1}>
          {volEntries.map(v => (
            <Box key={v.name} width={volColW}>
              <Text color={v.active ? 'green' : 'gray'}>{v.active ? '●' : '○'} </Text>
              <Text color="gray">{v.displayName}: </Text>
              <Text color="gray" bold>{v.size}</Text>
            </Box>
          ))}
        </Box>
      )}

      <Box gap={1} marginTop={1}>
        <Text color="gray" bold>DOCKER DISK</Text>
        {dockerDisk === null && <Text color="gray">calculating…</Text>}
      </Box>
      {dockerDisk !== null && (
        <Box gap={3} paddingLeft={1}>
          <Box gap={1}>
            <Text color="gray">images:</Text>
            <Text color="gray" bold>{dockerDisk.images || '—'}</Text>
          </Box>
          <Box gap={1}>
            <Text color="gray">volumes:</Text>
            <Text color="gray" bold>{dockerDisk.volumes || '—'}</Text>
          </Box>
          <Box gap={1}>
            <Text color="gray">build cache:</Text>
            <Text color="gray" bold>{dockerDisk.buildCache || '—'}</Text>
          </Box>
        </Box>
      )}
    </Box>
  )
})
